from functools import cached_property

import frappe
from insights.insights.doctype.insights_data_source.insights_data_source import (
    InsightsDataSource,
)
from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from insights.insights.doctype.insights_data_source.sources.frappe_db import FrappeDB
from insights.insights.doctype.insights_data_source.sources.mariadb import MariaDB
from insights.insights.doctype.insights_query.insights_query import InsightsQuery
from insights.insights.doctype.insights_table.insights_table import InsightsTable
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


class CustomInsightsQuery(InsightsQuery):
    def fetch_results(self):
        if self.flags.fetching_results:
            return []

        self.flags.fetching_results = True
        out = super().fetch_results()
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
