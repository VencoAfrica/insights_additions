from functools import cached_property
from insights import notify
from insights.insights.doctype.insights_data_source.insights_data_source import _sync_data_source
import frappe
from insights.insights.doctype.insights_data_source.insights_data_source import (
    InsightsDataSource,
)
from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from frappe.desk.doctype.tag.tag import add_tag
from insights.insights.doctype.insights_query.insights_query import InsightsQuery
from insights.insights.doctype.insights_table.insights_table import InsightsTable
from insights_changes.data_source import VirtualDB
from insights_changes.overrides.api import get_tables
from insights_changes.utils import (
    add_data_source_column_to_table_columns,
    get_columns_for_virtual_table,
    split_virtual_table_name,
)
from insights.api.setup import add_database


def add_venco_tag(doc, method=None):
    if frappe.get_doc("Tag", "VENCO Site"):
        add_tag("VENCO Site", doc.doctype, doc.name)
        return True


def add_data_source_table(doc, method=None):
    if doc.status == "Active":
        data_source = doc
        add_venco_tag(doc)
        data_source.enqueue_sync_tables_for_end_point()
        return doc, method


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
            if virtual:= frappe.db.exists("Insights Data Source", data_source):
                    # TODO: confirm the data source is part of a virtual data source
                 # this may not be necessary if tags are used to link sources in virtual data source
                self.virtual_data_source = virtual
                # load regular data source
                args = (*args[:to_replace], table_name, *args[to_replace + 1:])  # noqa: E203

        # call original init
        super().__init__(*args, **kwargs)

    def __setup__(self):
        """add `Data Source` column"""
        if not self.virtual_data_source:
            return

        # TODO: avoid saving this to database
        # add columns from other tables for same virtual data source
        self.columns = get_columns_for_virtual_table(self)
        add_data_source_column_to_table_columns(self)

    def get_columns(self):
        super().get_columns()
        if self.virtual_data_source:
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

    @cached_property
    def db(self) -> BaseDatabase:
        if self.composite_datasource:
            return VirtualDB(self.name, self)
        return super().db

    def validate(self):
        if self.composite_datasource:
            return
        frappe.log_error("Doc", "We Got here 4")
        return super().validate()

    def get_insights_table_preview(self, table, limit=100):
        db = self.db
        if isinstance(db, VirtualDB):
            return db.get_insights_table_preview(table, limit)

        db_table = frappe.get_value("Insights Table", table, "table")
        return self.get_table_preview(db_table, limit)

    @frappe.whitelist()
    def enqueue_sync_tables_for_end_point(self):
        from frappe.utils.scheduler import is_scheduler_inactive

        if is_scheduler_inactive():
            notify(
                **{
                    "title": "Error",
                    "message": "Scheduler is inactive",
                    "type": "error",
                }
            )

        frappe.enqueue(
            _sync_data_source,
            data_source=self.name,
            job_name="sync_data_source",
            queue="long",
            timeout=3600,
        )


class CustomInsightsQuery(InsightsQuery):
    @frappe.whitelist()
    def fetch_tables(self):
        with_query_tables = frappe.db.get_single_value(
            "Insights Settings", "allow_subquery")
        return get_tables(self.data_source, with_query_tables)

    @frappe.whitelist()
    def fetch_columns(self):
        if self.is_native_query:
            return []

        columns = []
        selected_tables = self.get_selected_tables()
        virtual_data_source = frappe.db.get_value(
            "Insights Data Source", {
                "name": self.data_source, "composite_datasource": 1}
        )
        for table in selected_tables:
            table_doc = frappe.get_doc(
                "Insights Table", {"table": table.get("table")})
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
