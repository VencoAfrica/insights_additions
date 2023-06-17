import frappe
from insights.api import get_data_sources as get_data_source_original
from insights.decorators import check_role
from insights.insights.doctype.insights_team.insights_team import (
    check_data_source_permission,
    get_permission_filter,
)
from insights_changes.utils import get_sources_for_virtual, make_virtual_table_name

fields_for_get_all_tables = ["name", "table", "label", "hidden"]


@frappe.whitelist()
@check_role("Insights User")
def get_data_source(name):
    check_data_source_permission(name)
    doc = frappe.get_doc("Insights Data Source", name)
    if not doc.composite_datasource:
        return get_data_source_original(name)

    # get tables for each source and combine
    seen = {}
    for source in get_sources_for_virtual(name, get_docs=False):
        tables = get_all_tables(source)
        for table in tables:
            key = tuple(table[k] for k in fields_for_get_all_tables[1:])
            # TODO: how to decide which table to incude? ie which data source is the reference
            if key not in seen:
                # if table for one source is hidden and another isn't, set as not hidden
                # TODO: update
                if (*key[:3], 1 - key[-1]) in seen:
                    if not key[-1]:
                        continue
                    seen.pop(key)
                table["name"] = make_virtual_table_name(table["name"], name)
                seen[key] = table

    return {
        "doc": doc.as_dict(),
        "tables": list(seen.values()),
    }


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
                table["name"] = make_virtual_table_name(
                    table["name"], data_source)
                seen[key] = table

    return list(seen.values())
