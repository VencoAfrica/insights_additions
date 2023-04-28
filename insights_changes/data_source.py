import json

import frappe
import pandas as pd
from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from insights.insights.query_builders.sql_builder import SQLQueryBuilder
from insights_changes.utils import get_sources_for_virtual, set_table_columns_for_df


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

    def get_table_preview(self, insights_table, limit=100):
        db_table = frappe.get_value("Insights Table", insights_table, "table") or insights_table
        total_length = 0
        df = pd.DataFrame()
        for source_doc in get_sources_for_virtual(self.data_source):
            data = source_doc.db.execute_query(
                f"""select * from `{db_table}` limit {limit}""", return_columns=True
            )
            columns = data.pop(0)
            column_names = [col["label"] for col in columns]
            new_df = pd.DataFrame(data)
            new_df.columns = column_names
            new_df.insert(0, "data_source", source_doc.name)
            df = pd.concat([df, new_df], ignore_index=True)
            length = source_doc.db.execute_query(f"""select count(*) from `{db_table}`""")[0][0]
            total_length += length

        # ensure columns order match that in InsightsTable.columns
        df = set_table_columns_for_df(df, insights_table, self.data_source)
        return {
            "data": json.loads(df.to_json(orient="values", date_format="iso")),
            "length": total_length,
        }

    def execute_query(
        self,
        sql,
        pluck=False,
        return_columns=False,
        replace_query_tables=False,
        is_native_query=False,
    ):
        results = []
        for source_doc in get_sources_for_virtual(self.data_source):
            results.append(
                source_doc.db.execute_query(
                    sql, pluck, return_columns, replace_query_tables, is_native_query
                )
            )
        return results

    # def get_table_columns(self, table):
    #     return super().get_table_columns(table)

    # def get_column_options(self, table, column, search_text=None, limit=50):
    #     return super().get_column_options(table, column, search_text, limit=50)
