import frappe
from frappe.desk.doctype.tag.tag import add_tag as add_tag_original
from insights.decorators import check_role
from insights.insights.doctype.insights_team.insights_team import (
    check_data_source_permission,
    check_table_permission,
    get_permission_filter,
)
from insights_additions.utils import (
    get_sources_for_virtual,
    make_virtual_table_name,
    validate_no_cycle_in_sources,
)

fields_for_get_all_tables = ["name", "table", "label", "hidden"]


def get_all_tables(data_source=None):
    if not data_source:
        return []

    return frappe.get_list(
        "Insights Table",
        filters={
            "data_source": data_source,
            **get_permission_filter("Insights Table"),
        },
        fields=fields_for_get_all_tables,
        order_by="hidden asc, label asc",
    )


@frappe.whitelist()
@check_role("Insights User")
def get_tables(data_source=None, with_query_tables=False):
    if not data_source:
        return []

    if not frappe.db.get_value("Insights Data Source", data_source, "composite_datasource"):
        from insights.api import get_tables as _get_tables

        return _get_tables(data_source=data_source, with_query_tables=with_query_tables)

    check_data_source_permission(data_source)
    filters = {
        "hidden": 0,
        **get_permission_filter("Insights Table"),
    }
    if not with_query_tables:
        filters["is_query_based"] = 0

    # get tables for each source and combine
    fields = ["name", "table", "label", "is_query_based"]
    seen = {}
    for source in get_sources_for_virtual(data_source, get_docs=False):
        tables = frappe.get_list(
            "Insights Table",
            filters={**filters, "data_source": source},
            fields=fields,
            order_by="is_query_based asc, label asc",
        )
        for table in tables:
            key = tuple(table[k] for k in fields[1:])
            # TODO: how to decide which table to incude? ie which data source is the reference
            if key not in seen:
                # if table for one source is hidden and another isn't, set as not hidden
                # TODO: update
                if (*key[:3], 1 - key[-1]) in seen:
                    if not key[-1]:
                        continue
                    seen.pop(key)
                table["name"] = make_virtual_table_name(table["name"], data_source)
                seen[key] = table

    return list(seen.values())


@frappe.whitelist()
def add_tag(tag, dt, dn, color=None):
    out = add_tag_original(tag, dt, dn, color=color)
    if dt == "Insights Data Source":
        validate_no_cycle_in_sources(dn)
    return out


@frappe.whitelist()
def get_queries_column(query_names):
    # TODO: handle permissions
    tables = {}
    for query in list(set(query_names)):
        # TODO: to further optimize, store the used tables in the query on save
        doc = frappe.get_cached_doc("Insights Query", query)
        virtual_data_source = frappe.db.get_value(
            "Insights Data Source", {"name": doc.data_source, "composite_datasource": 1}
        )
        for table in doc.get_selected_tables():
            tables[table.table] = (table, virtual_data_source)

    columns = []
    for table, virtual_data_source in tables.values():
        doc = frappe.get_cached_doc("Insights Table", {"table": table.table})
        doc.virtual_data_source = virtual_data_source
        _columns = doc.get_columns()
        for column in _columns:
            columns.append(
                {
                    "column": column.column,
                    "label": column.label,
                    "table": table.table,
                    "table_label": table.label,
                    "type": column.type,
                    "data_source": virtual_data_source or doc.data_source,
                }
            )

    return columns


@frappe.whitelist()
@check_role("Insights User")
def get_table_name(data_source, table):
    check_table_permission(data_source, table)
    for source in get_sources_for_virtual(data_source, get_docs=False, as_generator=True):
        name = frappe.get_value("Insights Table", {"data_source": source, "table": table}, "name")
        if name:
            return make_virtual_table_name(name, data_source)
