from functools import cached_property
import frappe

#reimplementing insight query with custom changes
import time
from json import dumps
import pandas as pd
import sqlparse
from frappe.model.document import Document
from frappe.utils import flt
from insights.decorators import log_error
from insights.insights.doctype.insights_data_source.sources.utils import (
    create_insights_table,
)
from insights.utils import ResultColumn
from insights.insights.doctype.insights_data_source.sources.query_store import sync_query_store
from insights.insights.doctype.insights_query.insights_query import InsightsQueryValidation

DEFAULT_FILTERS = dumps(
    {
        "type": "LogicalExpression",
        "operator": "&&",
        "level": 1,
        "position": 1,
        "conditions": [],
    },
    indent=2,
)
######

from insights.insights.doctype.insights_data_source.insights_data_source import (
    InsightsDataSource,
)
from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from insights.insights.doctype.insights_data_source.sources.frappe_db import FrappeDB
from insights.insights.doctype.insights_data_source.sources.mariadb import MariaDB
from insights.insights.doctype.insights_table.insights_table import InsightsTable
from insights_changes.overrides.doctype.query_client import CustomInsightsQueryClient
from insights_changes.data_source import VirtualDB
from insights_changes.overrides.functions import get_tables
from insights_changes.overrides.functions.is_frappe_db import is_frappe_db
from insights_changes.utils import (
    add_data_source_column_to_table_columns,
    get_columns_for_virtual_table,
    split_virtual_table_name,
    validate_no_cycle_in_sources,
)
class CustomInsightsTable(InsightsTable):
    """`Virtual InsightsTable`"""

    def __init__(self, *args, **kwargs):
        """
        instantiate a virtual InsightsTable doc

        Name should be given as <regular-table-name::virtual-datasource>
            eg: tabNote::virtual_1
        """

        self.virtual_data_source = None
        if args and isinstance(args[0], str):
            name, to_replace = (args[0], 0) if len(args) == 1 else (args[1], 1)
            table_name, data_source = split_virtual_table_name(name)
            if virtual := frappe.db.exists("Insights Data Source", data_source):
                # TODO: confirm the data source is part of a virtual data source
                # this may not be necessary if tags are used to link sources in virtual data source
                self.virtual_data_source = virtual
                # load regular data source
                args = (*args[:to_replace], table_name, *args[to_replace + 1 :])  # noqa: E203

        # call original init
        super().__init__(*args, **kwargs)

    def __setup__(self):
        """add `Data Source` column"""
        if not self.virtual_data_source:
            return

        # TODO: avoid saving this to database
        # add columns from other tables for same virtual data source
        self.columns = get_columns_for_virtual_table(self)
        self.flags.columns_for_virtual_table_gotten = True
        add_data_source_column_to_table_columns(self)

    def get_columns(self):
        if not self.virtual_data_source:
            return super().get_columns()

        if not self.flags.columns_for_virtual_table_gotten:
            self.__setup__()
            return self.columns

        super().get_columns()
        add_data_source_column_to_table_columns(self)
        return self.columns

    @frappe.whitelist()
    def get_preview(self):
        data_source = frappe.get_doc(
            "Insights Data Source", self.virtual_data_source or self.data_source
        )
        # use self.name instead of self.table (data_source.get_table_preview)
        return data_source.get_insights_table_preview(self.name)
    
class CustomInsightsDataSource(InsightsDataSource):
    """`Virtual InsightsTable`"""

    def get_database(self):
        conn_args = {
            "data_source": self.name,
            "host": self.host,
            "port": self.port,
            "use_ssl": self.use_ssl,
            "username": self.username,
            "password": self.get_password(),
            "database_name": self.database_name,
        }

        if is_frappe_db(conn_args):
            return FrappeDB(**conn_args)

        if self.database_type == "MariaDB":
            return MariaDB(**conn_args)

        frappe.throw(f"Unsupported database type: {self.database_type}")

    @cached_property
    def db(self) -> BaseDatabase:
        if self.composite_datasource:
            return VirtualDB(self.name, self)
        return super().db

    def validate(self):
        if self.composite_datasource:
            validate_no_cycle_in_sources(self)
            return
        return super().validate()

    def get_insights_table_preview(self, table, limit=100):
        db = self.db
        if isinstance(db, VirtualDB):
            return db.get_insights_table_preview(table, limit)

        db_table = frappe.get_value("Insights Table", table, "table")
        return self.get_table_preview(db_table, limit)

    def add_tag(self, tag):
        out = super().add_tag(tag)
        validate_no_cycle_in_sources(self)
        return out

class CustomInsightsQuery(InsightsQueryValidation, CustomInsightsQueryClient, Document):
    ## these are the custom functions that are being overridden lines 157 - 199
    def fetch_results(self):
        if self.flags.fetching_results:
            return []

        self.flags.fetching_results = True
        out = self.fetch_results_original()
        del self.flags.fetching_results
        return out

    @frappe.whitelist()
    def fetch_tables(self):
        with_query_tables = frappe.db.get_single_value("Insights Settings", "allow_subquery")
        return get_tables(self.data_source, with_query_tables)

    @frappe.whitelist()
    def fetch_columns(self):
        if self.is_native_query:
            return []

        columns = []
        selected_tables = self.get_selected_tables()
        virtual_data_source = frappe.db.get_value(
            "Insights Data Source", {"name": self.data_source, "composite_datasource": 1}
        )
        for table in selected_tables:
            table_doc = frappe.get_doc("Insights Table", {"table": table.get("table")})
            table_doc.virtual_data_source = virtual_data_source
            _columns = table_doc.get_columns()
            columns += [
                frappe._dict(
                    {
                        "table": table.get("table"),
                        "table_label": table.get("label"),
                        "column": c.get("column"),
                        "label": c.get("label"),
                        "type": c.get("type"),
                        "data_source": self.data_source,
                    }
                )
                for c in _columns
            ]
        return columns

####original code below
    def before_save(self):
        if not self.tables and not self.is_native_query:
            return self.clear()

        if self.get("skip_before_save"):
            return

        self.update_query()

    def on_update(self):
        self.update_insights_table()
        self.sync_query_store()
        self.update_link_docs_title()
        # TODO: update result columns on update

    def on_trash(self):
        self.delete_insights_table()
        self.delete_insights_charts()

    @property
    def _data_source(self):
        return frappe.get_doc("Insights Data Source", self.data_source)

    @property
    def results(self) -> str:
        LIMIT = (
            frappe.db.get_single_value("Insights Settings", "query_result_limit")
            or 1000
        )
        try:
            cached_results = self.load_results()
            if not cached_results and self.status == "Execution Successful":
                results = self.fetch_results()
                return frappe.as_json(results[:LIMIT])

            return frappe.as_json(cached_results[:LIMIT])
        except Exception as e:
            print("Error getting results", e)

    @property
    def results_row_count(self):
        return len(self.load_results() or [])

    def update_query(self):
        query = self._data_source.build_query(query=self)
        query = format_query(query) if query else None
        # in case of native query, the query doesn't get updated if the limit is changed
        # so we need to check if the limit is changed
        # because the native query is limited by the limit field
        limit_changed = (
            self.get_doc_before_save()
            and self.limit != self.get_doc_before_save().limit
        )
        if self.sql != query or limit_changed:
            self.sql = query
            self.status = "Pending Execution"

    def fetch_results_original(self):
        self.sync_child_stored_queries()
        start = time.monotonic()
        results = []
        try:
            results = self._data_source.run_query(query=self)
            results = self.process_column_types(results)
            results = self.apply_transformations(results)
            self.execution_time = flt(time.monotonic() - start, 3)
            self.last_execution = frappe.utils.now()
            self.status = "Execution Successful"
        except Exception:
            self.status = "Pending Execution"
            print("Error fetching results")
            raise

        self.store_results(results)
        return results

    def sync_child_stored_queries(self):
        if self.data_source == "Query Store" and self.tables:
            sync_query_store(
                [row.table for row in self.tables if row.table != self.name], force=True
            )

    def store_results(self, results):
        frappe.cache().set_value(
            f"insights_query|{self.name}",
            frappe.as_json(results),
        )

    def load_results(self, fetch_if_not_exists=False):
        results = frappe.cache().get_value(f"insights_query|{self.name}")
        if not results and fetch_if_not_exists:
            results = self.fetch_results()
        if not results:
            return []
        return frappe.parse_json(results)

    def update_link_docs_title(self):
        old_title = self.get("_doc_before_save") and self.get("_doc_before_save").title
        if old_title and old_title != self.title:
            # this still doesn't updates the old title stored the query column
            Table = frappe.qb.DocType("Insights Table")
            frappe.qb.update(Table).set(Table.label, self.title).where(
                Table.table == self.name
            ).run()

    def delete_insights_table(self):
        if table_name := frappe.db.exists("Insights Table", {"table": self.name}):
            frappe.delete_doc("Insights Table", table_name, ignore_permissions=True)

    def delete_insights_charts(self):
        charts = frappe.get_all(
            "Insights Chart",
            filters={"query": self.name},
            fields=["name"],
        )
        for chart in charts:
            frappe.delete_doc("Insights Chart", chart.name, ignore_permissions=True)

    def clear(self):
        self.tables = []
        self.columns = []
        self.filters = DEFAULT_FILTERS
        self.sql = ""
        self.limit = 500
        self.execution_time = 0
        self.last_execution = None
        frappe.cache().delete_value(f"insights_query|{self.name}")
        self.status = "Execution Successful"

    def sync_query_store(self):
        if self.is_stored:
            sync_query_store(tables=[self.name], force=True)

    @log_error()
    def process_column_types(self, results):
        if not results:
            return results

        if not self.is_native_query:
            results[0] = [ResultColumn.make(query_column=c) for c in self.get_columns()]
            return results

        columns = results[0]
        rows_df = pd.DataFrame(results[1:], columns=[c["label"] for c in columns])
        # create a row that contains values in each column
        values_row = []
        for column in rows_df.columns:
            # find the first non-null value in the column
            value = rows_df[column].dropna().iloc[0]
            values_row.append(value)

        # infer the type of each column
        inferred_types = self.guess_types(values_row)
        # update the column types
        for i, column in enumerate(columns):
            columns[i] = ResultColumn.make(
                label=column["label"],
                type=inferred_types[i],
            )
        results[0] = columns
        return results

    def guess_types(self, values):
        # try converting each value to a number, float, date, datetime
        # if it fails, it's a string
        types = []
        for value in values:
            types.append(guess_type(value))
        return types

    def update_insights_table(self):
        create_insights_table(
            frappe._dict(
                {
                    "table": self.name,
                    "label": self.title,
                    "data_source": self.data_source,
                    "is_query_based": 1,
                    "columns": [
                        frappe._dict(
                            {
                                "column": column.label,  # use label as column name
                                "label": column.label,
                                "type": column.type,
                            }
                        )
                        for column in self.get_columns()
                    ],
                }
            )
        )

    def get_columns(self):
        if not self.is_native_query:
            return self.columns or self.fetch_columns()

        # make column from results first row
        results = self.load_results(fetch_if_not_exists=True)
        if not results:
            return []
        return [
            frappe._dict(
                {
                    "label": col["label"],
                    "type": col["type"],
                }
            )
            for col in results[0]
        ]

    def apply_transformations(self, results):
        if self.is_native_query:
            return results
        if self.transforms:
            results = self.apply_transform(results)
        if self.has_cumulative_columns():
            results = self.apply_cumulative_sum(results)
        return results

    def apply_transform(self, results):
        self.validate_transforms()

        for row in self.transforms:
            if row.type == "Pivot":
                result = frappe.parse_json(results)
                options = frappe.parse_json(row.options)
                pivot_column = options.get("column")
                index_column = options.get("index")
                index_column_type = next(
                    (c.type for c in self.columns if c.label == index_column),
                    None,
                )
                index_column_options = next(
                    (c.format_option for c in self.columns if c.label == index_column),
                    None,
                )
                index_column_options = frappe.parse_json(index_column_options)
                value_column = options.get("value")
                value_column_type = next(
                    (c.type for c in self.columns if c.label == value_column),
                    None,
                )

                if not (pivot_column and index_column and value_column):
                    frappe.throw("Invalid Pivot Options")
                if pivot_column == index_column:
                    frappe.throw("Pivot Column and Index Column cannot be same")

                results_df = pd.DataFrame(
                    result[1:], columns=[d["label"] for d in result[0]]
                )

                pivot_column_values = results_df[pivot_column]
                index_column_values = results_df[index_column]
                value_column_values = results_df[value_column]

                # make a dataframe for pivot table
                pivot_df = pd.DataFrame(
                    {
                        index_column: index_column_values,
                        pivot_column: pivot_column_values,
                        value_column: value_column_values,
                    }
                )

                pivoted = pivot_df.pivot_table(
                    index=[pivot_df.columns[0]],
                    columns=[pivot_df.columns[1]],
                    values=[pivot_df.columns[2]],
                    aggfunc="sum",
                )

                pivoted.columns = pivoted.columns.droplevel(0)
                pivoted = pivoted.reset_index()
                pivoted.columns.name = None
                pivoted = pivoted.fillna(0)

                cols = pivoted.columns.to_list()
                index_result_column = ResultColumn.make(
                    cols[0], index_column_type, index_column_options
                )
                other_columns = [
                    ResultColumn.make(c, value_column_type) for c in cols[1:]
                ]
                cols = [index_result_column] + other_columns
                data = pivoted.values.tolist()

                return [cols] + data

            if row.type == "Unpivot":
                options = frappe.parse_json(row.options)
                index_column = options.get("index_column")
                new_column_name = options.get("column_label")
                value_name = options.get("value_label")

                if not (index_column and new_column_name and value_name):
                    frappe.throw("Invalid Unpivot Options")

                result = frappe.parse_json(results)
                columns = [c["label"] for c in result[0]]
                results_df = pd.DataFrame(result[1:], columns=columns)

                unpivoted = pd.melt(
                    results_df,
                    id_vars=[index_column],
                    var_name=new_column_name,
                    value_name=value_name,
                )

                index_column_type = next(
                    (c.type for c in self.columns if c.label == index_column),
                    None,
                )
                index_column_options = next(
                    (c.format_option for c in self.columns if c.label == index_column),
                    None,
                )
                index_column_options = frappe.parse_json(index_column_options)
                new_column_type = "String"
                value_column_type = "Decimal"

                cols = unpivoted.columns.to_list()
                cols = [
                    ResultColumn.make(cols[0], index_column_type, index_column_options),
                    ResultColumn.make(cols[1], new_column_type),
                    ResultColumn.make(cols[2], value_column_type),
                ]

                data = unpivoted.values.tolist()

                return [cols] + data

            if row.type == "Transpose":

                options = frappe.parse_json(row.options)
                index_column = options.get("index_column")
                new_column_label = options.get("column_label")

                if not index_column:
                    frappe.throw("Invalid Transpose Options")

                result = frappe.parse_json(results)
                columns = [c["label"] for c in result[0]]
                results_df = pd.DataFrame(result[1:], columns=columns)
                results_df = results_df.set_index(index_column)
                results_df_transposed = results_df.transpose()
                results_df_transposed = results_df_transposed.reset_index()
                results_df_transposed.columns.name = None

                cols = results_df_transposed.columns.to_list()
                index_result_column = ResultColumn.make(new_column_label, "String")
                other_columns = [ResultColumn.make(c, "Decimal") for c in cols[1:]]
                cols = [index_result_column] + other_columns
                data = results_df_transposed.values.tolist()

                return [cols] + data

    def validate_transforms(self):
        pivot_transforms = [t for t in self.transforms if t.type == "Pivot"]
        unpivot_transforms = [t for t in self.transforms if t.type == "Unpivot"]
        transpose_transforms = [t for t in self.transforms if t.type == "Transpose"]

        if len(pivot_transforms) > 1:
            frappe.throw("Only one Pivot transform is allowed")
        if len(unpivot_transforms) > 1:
            frappe.throw("Only one Unpivot transform is allowed")
        if len(transpose_transforms) > 1:
            frappe.throw("Only one Transpose transform is allowed")
        if pivot_transforms and unpivot_transforms:
            frappe.throw("Pivot and Unpivot transforms cannot be used together")
        if pivot_transforms and transpose_transforms:
            frappe.throw("Pivot and Transpose transforms cannot be used together")
        if unpivot_transforms and transpose_transforms:
            frappe.throw("Unpivot and Transpose transforms cannot be used together")

    def has_cumulative_columns(self):
        return any(
            col.aggregation and "Cumulative" in col.aggregation
            for col in self.get_columns()
        )

    def apply_cumulative_sum(self, results):
        result = frappe.parse_json(results)
        results_df = pd.DataFrame(result[1:], columns=[d["label"] for d in result[0]])

        for column in self.columns:
            if "Cumulative" in column.aggregation:
                results_df[column.label] = results_df[column.label].cumsum()

        return [result[0]] + results_df.values.tolist()
    
def format_query(query):
    return sqlparse.format(
        str(query),
        keyword_case="upper",
        reindent_aligned=True,
    )

def guess_type(value):
    try:
        pd.to_numeric(value)
        return "Integer"
    except ValueError:
        try:
            pd.to_numeric(value, downcast="float")
            return "Decimal"
        except ValueError:
            try:
                pd.to_datetime(value)
                return "Datetime"
            except ValueError:
                return "String"