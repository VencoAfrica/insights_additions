import concurrent.futures
import json

import frappe
import pandas as pd
from frappe.utils import unique
from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from insights.insights.query_builders.sql_builder import SQLQueryBuilder
from insights_changes.utils import (
    get_sources_for_virtual,
    merge_query_results,
    query_with_columns_in_table,
    set_table_columns_for_df,
)

SERIAL_LIMIT = 3


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
    def __init__(self, data_source, data_source_doc=None):
        self.data_source = data_source
        self.data_source_doc = data_source_doc
        self.query_builder: SQLQueryBuilder = SQLQueryBuilder()
        self.table_factory: VirtualTableFactory = VirtualTableFactory(data_source)

    # def sync_tables(self, tables=None, force=False):
    #     return super().sync_tables(tables, force)

    def get_source_docs(self, get_docs=True, as_generator=False):
        return get_sources_for_virtual(
            self.data_source_doc or self.data_source, get_docs=get_docs, as_generator=as_generator
        )

    def get_insights_table_preview(self, insights_table, limit=100):
        db_table = frappe.get_value("Insights Table", insights_table, "table") or insights_table
        source_docs = self.get_source_docs()
        results = []

        def get_data_and_length(source_doc):
            data = source_doc.db.execute_query(
                f"""select * from `{db_table}` limit {limit}""", return_columns=True
            )
            length = source_doc.db.execute_query(f"""select count(*) from `{db_table}`""")[0][0]
            return data, length

        def run_serial():
            for source_doc in source_docs:
                data, length = get_data_and_length(source_doc)
                results.append((source_doc.name, data, length))

        def run_concurrent():
            site = str(frappe.local.site)

            def get_data(source_doc):
                frappe.connect(site=site)
                data, length = get_data_and_length(source_doc)
                return data, length

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(get_data, doc): doc.name for doc in source_docs}
                for future in concurrent.futures.as_completed(futures):
                    source_docname = futures[future]
                    try:
                        data, length = future.result()
                    except Exception:
                        frappe.log_error(
                            "Data source: %r generated an exception: %s"
                            % (source_docname, frappe.get_traceback(with_context=True)),
                            "VirtualDB.get_insights_table_preview.run_concurrent",
                        )
                    else:
                        results.append((source_docname, data, length))

        if len(source_docs) <= SERIAL_LIMIT:
            run_serial()
        else:
            run_concurrent()

        total_length = 0
        df = pd.DataFrame()
        for source_docname, data, length in results:
            columns = data.pop(0)
            column_names = [col["label"] for col in columns]
            new_df = pd.DataFrame(data)
            new_df.columns = column_names
            new_df.insert(0, "data_source", source_docname)
            df = pd.concat([df, new_df], ignore_index=True)
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
        for source_doc in self.get_source_docs():
            results.append(
                source_doc.db.execute_query(
                    sql, pluck, return_columns, replace_query_tables, is_native_query
                )
            )
        return results

    def build_query(self, query):
        for source_doc in self.get_source_docs(get_docs=True, as_generator=True):
            return source_doc.build_query(query)

    def run_query(self, query):
        results = []
        source_docs = self.get_source_docs(get_docs=True)

        def run_serial():
            for source_doc in source_docs:
                new_query = query_with_columns_in_table(query, source_doc.name)
                result = source_doc.db.run_query(new_query)
                results.append((source_doc.name, result))

        def run_concurrent():
            site = str(frappe.local.site)

            def get_data(source_doc):
                frappe.connect(site=site)
                new_query = query_with_columns_in_table(query, source_doc.name)
                return source_doc.db.run_query(new_query)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(get_data, doc): doc.name for doc in source_docs}
                for future in concurrent.futures.as_completed(futures):
                    source_docname = futures[future]
                    try:
                        data = future.result()
                    except Exception:
                        frappe.log_error(
                            "Data Source: %r generated an exception: %s"
                            % (source_docname, frappe.get_traceback(with_context=True)),
                            "VirtualDB.run_query.run_concurrent",
                        )
                    else:
                        results.append((source_docname, data))

        if len(source_docs) <= SERIAL_LIMIT:
            run_serial()
        else:
            run_concurrent()

        return merge_query_results(results, query)

    # def get_table_columns(self, table):
    #     return super().get_table_columns(table)

    def get_column_options(self, table, column, search_text=None, limit=50):
        if column == "data_source":
            return self.get_source_docs(get_docs=False)

        results = []
        source_docs = [
            doc
            for doc in self.get_source_docs(get_docs=True, as_generator=True)
            if frappe.db.get_value(
                "Insights Table",
                filters=[
                    ["data_source", "=", doc.name],
                    ["Insights Table Column", "column", "=", column],
                ],
            )
        ]
        limit_per_source = max(limit // len(source_docs), 5)

        def get_column_options(source_doc):
            return source_doc.db.get_column_options(
                table=table, column=column, search_text=search_text, limit=limit_per_source
            )

        def run_serial():
            for source_doc in source_docs:
                results.extend(get_column_options(source_doc) or [])

        def run_concurrent():
            site = str(frappe.local.site)

            def get_data(source_doc):
                frappe.connect(site=site)
                return get_column_options(source_doc) or []

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(get_data, doc): doc.name for doc in source_docs}
                for future in concurrent.futures.as_completed(futures):
                    source_docname = futures[future]
                    try:
                        data = future.result()
                    except Exception:
                        frappe.log_error(
                            "%r generated an exception: %s"
                            % (source_docname, frappe.get_traceback()),
                            "get_column_options",
                        )
                    else:
                        results.extend(data)

        if len(source_docs) <= SERIAL_LIMIT:
            run_serial()
        else:
            run_concurrent()
        return unique(results)
