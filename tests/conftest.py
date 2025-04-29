import pytest

class MockCursor:
    def __init__(self):
        self.executed = []
        self.columns = []
        self.copy_batches = []
        self.fetchone_value = 1

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return (self.fetchone_value,)

    def copy_to(self, table_or_view, schema, data, rows_per_batch):
        self.copy_batches.append((table_or_view, schema, list(data)))

    def commit(self):
        pass

class MockConnection:
    def __init__(self):
        self._cursor = MockCursor()

    def cursor(self):
        return self._cursor

class MockColumn:
    def __init__(self, name, typename):
        self.column_name = name
        self.type = MockType(typename)
        self.flags = 0

class MockType:
    def __init__(self, name):
        self.name = name

@pytest.fixture
def mock_cursor():
    return MockCursor()

@pytest.fixture
def mock_connection():
    return MockConnection()
