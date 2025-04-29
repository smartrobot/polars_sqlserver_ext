import polars as pl
from ._bulk_insert import polars_bulk_insert

def enable_sqlserver_extensions():
    """
    Enables SQL Server extensions for Polars DataFrame objects.
    After calling, you can do df.write_sqlserver(...)
    """
    def write_sqlserver(
        self,
        connection_or_cursor,
        table_name: str,
        schema: str = "dbo",
        batch_size: int = 5000,
        if_table_exists: str = "append",
        reset_identity: bool = False,
        column_string_sizes: dict | None = None,
    ):
        polars_bulk_insert(
            df=self,
            connection_or_cursor=connection_or_cursor,
            table_name=table_name,
            schema=schema,
            batch_size=batch_size,
            if_table_exists=if_table_exists,
            reset_identity=reset_identity,
            column_string_sizes=column_string_sizes,
        )

    pl.DataFrame.write_sqlserver = write_sqlserver
