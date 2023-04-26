from functools import cached_property

import frappe
from insights.insights.doctype.insights_data_source.insights_data_source import (
    InsightsDataSource,
)
from insights.insights.doctype.insights_data_source.sources.base_database import (
    BaseDatabase,
)
from insights.insights.doctype.insights_table.insights_table import InsightsTable
from insights_changes.data_source import VirtualDB


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
            parts = name.rsplit("::", 1)
            if virtual := (
                frappe.db.exists("Insights Data Source", parts[1]) if len(parts) == 2 else None
            ):
                # TODO: confirm the data source is part of a virtual data source
                # this may not be necessary if tags are used to link sources in virtual data source
                self.virtual_data_source = virtual
                # load regular data source
                args = (*args[:to_replace], parts[0], *args[to_replace + 1 :])  # noqa: E203

        # call original init
        super().__init__(*args, **kwargs)

    def __setup__(self):
        """add `Data Source` column"""
        # TODO: avoid saving this to database
        self.append(
            "columns",
            {"name": "*" * 10, "column": "data_source", "label": "Data Source", "type": "String"},
        )
        # make first row
        row = self.columns.pop()
        row.idx = -1
        self.columns.insert(0, row)
        # TODO: add missing columns from other data sources

    @frappe.whitelist()
    def get_preview(self):
        data_source = frappe.get_doc(
            "Insights Data Source", self.virtual_data_source or self.data_source
        )
        return data_source.get_table_preview(self.table)


class CustomInsightsDataSource(InsightsDataSource):
    """`Virtual InsightsTable`"""

    @cached_property
    def db(self) -> BaseDatabase:
        if self.is_virtual:
            return VirtualDB(self.name)
        return super().db

    def validate(self):
        if self.is_virtual:
            return
        return super().validate()
