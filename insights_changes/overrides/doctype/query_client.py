#reimplementing insights query client with custom changes
from copy import deepcopy
import frappe
from json import dumps
from frappe.utils import cint, cstr
from insights.api import get_tables
######
from insights_changes.utils import (
    get_sources_for_virtual
)

class CustomInsightsQueryClient:
     ## these are the custom functions that are being overridden lines 13 - 68
    @frappe.whitelist()
    def fetch_join_options(self, left_table, right_table):
        """
        Fetches the join options for the given left and right tables.

        Args:
            left_table (str): The name of the left table.
            right_table (str): The name of the right table.

        Returns:
            dict: A dictionary containing the left columns, right columns, and saved links.
                - left_columns (list): The columns from the left table involved in the join.
                - right_columns (list): The columns from the right table involved in the join.
                - saved_links (list): The saved links between the left and right tables.
        """
        print("the data_source is: ", self.data_source)
        print("is the data source a composite datasource? :", frappe.db.get_value("Insights Data Source", self.data_source, "composite_datasource"))
        print("")

        if not frappe.db.get_value("Insights Data Source", self.data_source, "composite_datasource"):
            ##call original function for fetching join options
            return self.fetch_join_options_original(left_table, right_table)
        
        links = []
        #get the first data source as sample source from the composite data source
        source = get_sources_for_virtual(self.data_source)[0]
        print("left_table: ", left_table)
        print("right_table: ", right_table)
        print("source: ", source)
        print("source.data_source: ", source.name)
        print("")
        left_doc = frappe.get_cached_doc(
            "Insights Table",
            {
                "table": left_table,
                "data_source": source.name,
            },
        )
        right_doc = frappe.get_cached_doc(
            "Insights Table",
            {
                "table": right_table,
                "data_source": source.name,
            },
        )

        links = []
        for link in left_doc.table_links:
            if link.foreign_table == right_table:
                link_dict = frappe._dict({
                    "left": link.primary_key,
                    "right": link.foreign_key,
                })
                if link_dict not in links:
                    links.append(link_dict)

        # Remove duplicate links
        # links = list({tuple(link.items()) for link in links})
                    
        left_columns = left_doc.get_columns()
        right_columns = right_doc.get_columns()

        final_left_columns = [column for left in links for column in left_columns if column.column == left.left]
        final_right_columns = [column for right in links for column in right_columns if column.column == right.right]

        return {
            "left_columns": final_left_columns,
            "right_columns": final_right_columns,
            "saved_links": links,
        }

####original code below
    @frappe.whitelist()
    def duplicate(self):
        new_query = frappe.copy_doc(self)
        new_query.save()
        return new_query.name

    @frappe.whitelist()
    def add_table(self, table):
        new_table = {
            "label": table.get("label"),
            "table": table.get("table"),
        }
        self.append("tables", new_table)
        self.save()

    @frappe.whitelist()
    def update_table(self, table):
        for row in self.tables:
            if row.get("name") != table.get("name"):
                continue

            if table.get("join"):
                row.join = dumps(
                    table.get("join"),
                    default=cstr,
                    indent=2,
                )
            else:
                row.join = ""

            self.save()
            return

    @frappe.whitelist()
    def remove_table(self, table):
        for row in self.tables:
            if row.get("name") == table.get("name"):
                self.remove(row)
                break

        self.save()

    @frappe.whitelist()
    def add_column(self, column):
        new_column = {
            "type": column.get("type"),
            "label": column.get("label"),
            "table": column.get("table"),
            "column": column.get("column"),
            "table_label": column.get("table_label"),
            "aggregation": column.get("aggregation"),
            "is_expression": column.get("is_expression"),
            "expression": dumps(column.get("expression"), indent=2),
            "format_option": dumps(column.get("format_option"), indent=2),
        }
        self.append("columns", new_column)
        self.save()

    @frappe.whitelist()
    def move_column(self, from_index, to_index):
        self.columns.insert(to_index, self.columns.pop(from_index))
        for row in self.columns:
            row.idx = self.columns.index(row) + 1
        self.save()

    @frappe.whitelist()
    def update_column(self, column):
        for row in self.columns:
            if row.get("name") == column.get("name"):
                row.type = column.get("type")
                row.label = column.get("label")
                row.table = column.get("table")
                row.column = column.get("column")
                row.order_by = column.get("order_by")
                row.aggregation = column.get("aggregation")
                row.table_label = column.get("table_label")
                row.aggregation_condition = column.get("aggregation_condition")
                format_option = column.get("format_option")
                if format_option:
                    # check if format option is an object
                    row.format_option = (
                        dumps(format_option, indent=2)
                        if isinstance(format_option, dict)
                        else format_option
                    )
                expression = column.get("expression")
                if expression:
                    # check if expression is an object
                    row.expression = (
                        dumps(expression, indent=2)
                        if isinstance(expression, dict)
                        else expression
                    )
                break

        self.save()

    @frappe.whitelist()
    def remove_column(self, column):
        for row in self.columns:
            if row.get("name") == column.get("name"):
                self.remove(row)
                break

        self.save()

    @frappe.whitelist()
    def update_filters(self, filters):
        sanitized_conditions = self.sanitize_conditions(filters.get("conditions"))
        filters["conditions"] = sanitized_conditions or []
        self.filters = dumps(filters, indent=2, default=cstr)
        self.save()

    def sanitize_conditions(self, conditions):
        if not conditions:
            return

        _conditions = deepcopy(conditions)

        for idx, condition in enumerate(_conditions):
            if "conditions" not in condition:
                # TODO: validate if condition is valid
                continue

            sanitized_conditions = self.sanitize_conditions(condition.get("conditions"))
            if sanitized_conditions:
                conditions[idx]["conditions"] = sanitized_conditions
            else:
                # remove the condition if it has zero conditions
                conditions.remove(condition)

        return conditions

    @frappe.whitelist()
    def add_transform(self, type, options):
        existing = self.get("transforms", {"type": type})
        if existing:
            existing[0].options = frappe.as_json(options)
        else:
            self.append(
                "transforms",
                {
                    "type": type,
                    "options": frappe.as_json(options),
                },
            )
        self.run()

    @frappe.whitelist()
    def reset_transforms(self):
        self.transforms = []
        self.run()

    @frappe.whitelist()
    def fetch_tables(self):
        with_query_tables = frappe.db.get_single_value(
            "Insights Settings", "allow_subquery"
        )
        return get_tables(self.data_source, with_query_tables)

    @frappe.whitelist()
    def fetch_columns(self):
        if self.is_native_query:
            return []

        columns = []
        selected_tables = self.get_selected_tables()
        for table in selected_tables:
            table_doc = frappe.get_doc("Insights Table", {"table": table.get("table")})
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

    def get_selected_tables(self):
        join_tables = []
        for table in self.tables:
            if table.join:
                join = frappe.parse_json(table.join)
                join_tables.append(
                    frappe._dict(
                        {
                            "table": join.get("with").get("value"),
                            "label": join.get("with").get("label"),
                        }
                    )
                )

        return self.tables + join_tables

    @frappe.whitelist()
    def set_limit(self, limit):
        sanitized_limit = cint(limit)
        if not sanitized_limit or sanitized_limit < 0:
            frappe.throw("Limit must be a positive integer")
        self.limit = sanitized_limit
        self.save()

    @frappe.whitelist()
    def fetch_column_values(self, column, search_text=None):
        data_source = frappe.get_doc("Insights Data Source", self.data_source)
        return data_source.get_column_options(
            column.get("table"), column.get("column"), search_text
        )

    print("running original function fetch_join_options_original")
    @frappe.whitelist()
    def fetch_join_options_original(self, left_table, right_table):
        left_doc = frappe.get_cached_doc(
            "Insights Table",
            {
                "table": left_table,
                "data_source": self.data_source,
            },
        )
        right_doc = frappe.get_cached_doc(
            "Insights Table",
            {
                "table": right_table,
                "data_source": self.data_source,
            },
        )

        links = []
        for link in left_doc.table_links:
            if link.foreign_table == right_table:
                links.append(
                    frappe._dict(
                        {
                            "left": link.primary_key,
                            "right": link.foreign_key,
                        }
                    )
                )

        return {
            "left_columns": left_doc.get_columns(),
            "right_columns": right_doc.get_columns(),
            "saved_links": links,
        }

    @frappe.whitelist()
    def run(self):
        if self.data_source == "Query Store":
            tables = (t.table for t in self.tables)
            subqueries = frappe.get_all(
                "Insights Query", {"name": ["in", tables]}, pluck="name"
            )
            for subquery in subqueries:
                frappe.get_doc("Insights Query", subquery).run()
        self.update_query()
        self.fetch_results()
        self.skip_before_save = True
        self.save()

    @frappe.whitelist()
    def reset(self):
        self.clear()
        self.skip_before_save = True
        self.save()

    @frappe.whitelist()
    def store(self):
        self.is_stored = 1
        self.save()

    @frappe.whitelist()
    def convert(self):
        self.is_native_query = not self.is_native_query
        self.save()

    @frappe.whitelist()
    def get_source_schema(self):
        return self._data_source.get_schema()