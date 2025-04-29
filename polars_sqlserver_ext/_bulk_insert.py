import polars as pl
import datetime
from decimal import Decimal
from typing import Optional, Union, Dict
import pytds
from pytds import tds_base
from concurrent.futures import ThreadPoolExecutor

def polars_bulk_insert(
    df: pl.DataFrame,
    connection_or_cursor: Union[pytds.Connection, pytds.Cursor],
    table_name: str,
    schema: str = "dbo",
    batch_size: int = 5000,
    if_table_exists: str = "append",
    reset_identity: bool = False,
    column_string_sizes: Optional[Dict[str, int]] = None,
) -> None:
    """
    Bulk insert a Polars DataFrame into SQL Server using pytds,
    with multithreaded batch preparation.
    """

    if df.is_empty():
        raise ValueError("DataFrame is empty, nothing to insert.")

    if if_table_exists not in {'append', 'replace', 'fail', 'delete', 'truncate'}:
        raise ValueError(
            f"Invalid if_table_exists='{if_table_exists}'. Must be one of "
            f"'append', 'replace', 'fail', 'delete', 'truncate'."
        )

    if reset_identity and if_table_exists not in {'delete', 'truncate'}:
        raise ValueError(
            f"reset_identity=True is only valid with if_table_exists='delete' or 'truncate'."
        )

    # Step 1: Get a cursor
    if isinstance(connection_or_cursor, pytds.Connection):
        cursor = connection_or_cursor.cursor()
    else:
        cursor = connection_or_cursor

    full_table_name = f"{schema}.{table_name}" if schema else table_name

    # Step 2: Check if table exists
    cursor.execute(f"SELECT OBJECT_ID('{full_table_name}', 'U')")
    table_exists = cursor.fetchone()[0] is not None

    if if_table_exists == 'fail' and table_exists:
        raise RuntimeError(f"Table {full_table_name} already exists.")

    if if_table_exists == 'replace' and table_exists:
        cursor.execute(f"DROP TABLE {full_table_name}")
        connection_or_cursor.commit()
        _create_table_from_polars(df, cursor, full_table_name, column_string_sizes)
        connection_or_cursor.commit()

    elif if_table_exists == 'replace' and not table_exists:
        _create_table_from_polars(df, cursor, full_table_name, column_string_sizes)
        connection_or_cursor.commit()

    elif if_table_exists == 'delete' and table_exists:
        cursor.execute(f"DELETE FROM {full_table_name}")
        connection_or_cursor.commit()
        if reset_identity:
            cursor.execute(f"DBCC CHECKIDENT ('{full_table_name}', RESEED, 0)")
            connection_or_cursor.commit()

    elif if_table_exists == 'truncate' and table_exists:
        cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        connection_or_cursor.commit()
        if reset_identity:
            cursor.execute(f"DBCC CHECKIDENT ('{full_table_name}', RESEED, 0)")
            connection_or_cursor.commit()

    # Step 3: Fetch column metadata
    cursor.execute(f"SELECT TOP 0 * FROM {full_table_name}")
    sql_columns = {col.column_name.lower(): col for col in cursor.columns}

    # Step 4: Multithreaded batch preparation
    num_workers = 4
    prefetch_batches = 2

    rows_iter = df.iter_rows(named=True)
    executor = ThreadPoolExecutor(max_workers=num_workers)
    futures = []

    for _ in range(prefetch_batches):
        future = executor.submit(_prepare_batch, rows_iter, batch_size, sql_columns)
        futures.append(future)

    while futures:
        done_future = futures.pop(0)
        batch = done_future.result()

        if batch:
            cursor.copy_to(
                table_or_view=table_name,
                schema=schema,
                data=batch,
                rows_per_batch=batch_size,
            )

        try:
            future = executor.submit(_prepare_batch, rows_iter, batch_size, sql_columns)
            futures.append(future)
        except StopIteration:
            pass

    executor.shutdown(wait=True)


def _prepare_batch(rows_iter, batch_size, sql_columns):
    """
    Prepares a single batch of processed rows.
    """
    batch = []
    try:
        for _ in range(batch_size):
            row = next(rows_iter)
            processed_row = []
            for colname, value in row.items():
                col_info = sql_columns.get(colname.lower())
                processed_value = _prepare_value(value, col_info)
                processed_row.append(processed_value)
            batch.append(tuple(processed_row))
    except StopIteration:
        pass
    return batch


def _prepare_value(value, col_info):
    """
    Prepares a single value for SQL Server based on target column metadata.
    """
    if value is None:
        return None
    if col_info.type.name == 'bit':
        return 1 if value else 0
    if col_info.type.name in ('decimaln', 'numericn'):
        if isinstance(value, Decimal):
            return value
        else:
            return Decimal(str(value))
    if col_info.type.name in ('datetime', 'datetime2', 'smalldatetime', 'date', 'time'):
        if isinstance(value, datetime.datetime):
            return value
        else:
            seconds = value / 1_000_000_000
            return datetime.datetime.utcfromtimestamp(seconds)
    return value


def _create_table_from_polars(
    df: pl.DataFrame,
    cursor,
    full_table_name: str,
    column_string_sizes: Optional[Dict[str, int]] = None
) -> None:
    """
    Creates a table in SQL Server based on Polars DataFrame schema.
    """
    sql_columns = []
    for name, dtype in df.schema.items():
        sql_type = map_polars_dtype_to_sql(dtype, name, column_string_sizes, df)
        sql_columns.append(f"[{name}] {sql_type}")
    columns_sql = ", ".join(sql_columns)
    cursor.execute(f"CREATE TABLE {full_table_name} ({columns_sql})")


def map_polars_dtype_to_sql(
    dtype: pl.DataType,
    colname: str,
    column_string_sizes: Optional[Dict[str, int]] = None,
    df: Optional[pl.DataFrame] = None
) -> str:
    """
    Maps Polars dtype to SQL Server type string.
    """
    if dtype == pl.Int8:
        return "TINYINT"
    elif dtype == pl.Int16:
        return "SMALLINT"
    elif dtype == pl.Int32:
        return "INT"
    elif dtype == pl.Int64:
        return "BIGINT"
    elif dtype == pl.UInt8:
        return "TINYINT"
    elif dtype == pl.UInt16:
        return "SMALLINT"
    elif dtype == pl.UInt32:
        return "INT"
    elif dtype == pl.UInt64:
        return "BIGINT"
    elif dtype == pl.Float32:
        return "REAL"
    elif dtype == pl.Float64:
        return "FLOAT"
    elif dtype == pl.Boolean:
        return "BIT"
    elif isinstance(dtype, pl.Datetime):
        return "DATETIME2"
    elif isinstance(dtype, pl.Date):
        return "DATE"
    elif isinstance(dtype, pl.Time):
        return "TIME"
    elif isinstance(dtype, pl.Duration):
        return "BIGINT"
    elif isinstance(dtype, pl.Decimal):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    elif dtype == pl.Binary:
        return "VARBINARY(MAX)"
    elif dtype == pl.Utf8 or dtype == pl.Categorical:
        if column_string_sizes and colname in column_string_sizes:
            length = column_string_sizes[colname]
        elif df is not None:
            sample = df[colname].drop_nulls()
            if sample.is_empty():
                length = 50
            else:
                max_len = sample.str.len_chars().max()
                length = max_len if max_len <= 4000 else None
        else:
            length = None

        if length is None:
            return "NVARCHAR(MAX)"
        else:
            return f"NVARCHAR({length})"
    elif dtype == pl.Null:
        return "NVARCHAR(1) NULL"
    else:
        raise ValueError(f"Unsupported Polars dtype: {dtype}")
