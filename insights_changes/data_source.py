from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from insights.insights.query_builders.sql_builder import SQLQueryBuilder
from insights_changes.utils import get_sources_for_virtual


class VirtualTableFactory:
    """Fetchs tables and columns from database and links from doctype"""

    def __init__(self, data_source) -> None:
        ...

    # def sync_tables(self, connection, tables, force=False):
    #     ...

    # def get_tables(self, table_names=None):
    #     ...

    # def get_db_tables(self, table_names=None):
    #     ...

    # def get_table(self, table_name):
    #     ...

    # def get_table_columns(self, table_name):
    #     ...


class VirtualDB(BaseDatabase):
    def __init__(self, data_source):
        self.data_source = data_source
        self.query_builder: SQLQueryBuilder = SQLQueryBuilder()
        self.table_factory: VirtualTableFactory = VirtualTableFactory(data_source)

    # def sync_tables(self, tables=None, force=False):
    #     return super().sync_tables(tables, force)

    def get_table_preview(self, table, limit=100):
        sources_data = []
        total_length = 0
        for source_doc in get_sources_for_virtual(self.data_source):
            source_data = source_doc.db.get_table_preview(table, limit)
            total_length += source_data["length"]
            sources_data.extend([[source_doc.name, *row] for row in source_data["data"]])
        return {
            "data": sources_data,
            "length": total_length,
        }

    # def get_table_columns(self, table):
    #     return super().get_table_columns(table)

    # def get_column_options(self, table, column, search_text=None, limit=50):
    #     return super().get_column_options(table, column, search_text, limit=50)
