import json
import operator

import frappe
import pandas as pd


def make_virtual_table_name(table_name, virtual_data_source_name):
    return f"{table_name}::{virtual_data_source_name}"


def split_virtual_table_name(virtual_table_name):
    if not virtual_table_name:
        return ("", "")
    if not isinstance(virtual_table_name, str):
        return (virtual_table_name, "")

    parts = tuple(virtual_table_name.rsplit("::", 1))
    if len(parts) < 2:
        return parts[0], ""
    return parts


def get_single_sources_for_virtual(data_source, get_docs=True, as_generator=False):
    if isinstance(data_source, str):
        data_source = frappe.get_doc("Insights Data Source", data_source)

    if not data_source.composite_datasource:
        return (_ for _ in ()) if as_generator else []

    tags = [row.tag for row in data_source.get("sources")]
    sources = (
        (frappe.get_doc("Insights Data Source", r[0]) if get_docs else r[0])
        for r in frappe.get_list(
            "Tag Link",
            filters={"tag": ["in", tags], "document_type": "Insights Data Source"},
            fields=["document_name"],
            as_list=1,
        )
    )

    return sources if as_generator else list(sources)


def get_nested_sources_for_virtual(data_source):
    if isinstance(data_source, str):
        data_source = frappe.get_doc("Insights Data Source", data_source)
    if not data_source.composite_datasource:
        return set(), None

    sources = set()
    parents = {data_source.name}
    children = set(get_single_sources_for_virtual(data_source, get_docs=False))
    while children:
        new_children = set()
        new_parents = set()
        for child_name in children:
            if child_name in parents:
                return sources, parents
            child = frappe.get_doc("Insights Data Source", child_name)
            new_children.update(get_single_sources_for_virtual(child, get_docs=False))
            if child.composite_datasource:
                new_parents.add(child_name)
        parents.update(new_parents)
        sources.update(children.difference(new_parents))
        children = new_children
    return sources, None


def get_sources_for_virtual(data_source, get_docs=True, as_generator=False):
    sources = (
        (frappe.get_doc("Insights Data Source", source) if get_docs else source)
        for source in get_nested_sources_for_virtual(data_source)[0]
    )

    return sources if as_generator else list(sources)


def get_columns_for_virtual_table(virtual_table, get_docs=True):
    if isinstance(virtual_table, str):
        virtual_table = frappe.get_doc("Insights Table", virtual_table)
    data_source = frappe.get_doc("Insights Data Source", virtual_table.virtual_data_source)

    columns = virtual_table.get("columns") or []
    if columns:
        columns = columns[:]
        column_names = set(col.column for col in columns)
        for source_doc in get_sources_for_virtual(data_source):
            idx = len(columns)
            if table_name := frappe.db.exists(
                "Insights Table",
                {
                    "data_source": source_doc.name,
                    "table": virtual_table.table,
                    "label": virtual_table.label,
                    "name": ["!=", virtual_table.name],
                },
            ):
                table_doc = frappe.get_doc("Insights Table", table_name)
                for col in reversed(table_doc.columns):
                    if col.column not in column_names:
                        column_names.add(col.column)
                        columns.insert(idx, col)
                    else:
                        idx = min(0, idx - 1)
    return columns


def add_data_source_column_to_table_columns(virtual_table):
    if isinstance(virtual_table, str):
        virtual_table = frappe.get_doc("Insights Table", virtual_table)

    data_source_col = {
        "name": "*" * 10,
        "column": "data_source",
        "label": "Data Source",
        "type": "String",
    }
    if cols := virtual_table.get("columns"):
        for key, val in data_source_col.items():
            if cols[0].get(key) != val:
                break
        else:
            return virtual_table

    virtual_table.append("columns", data_source_col)
    # make first row
    row = virtual_table.columns.pop()
    row.idx = -1
    virtual_table.columns.insert(0, row)
    return virtual_table


def set_table_columns_for_df(data_frame, table_name, virtual_data_source):
    # get columns for table
    virtual_table = frappe.get_doc(
        "Insights Table", make_virtual_table_name(table_name, virtual_data_source)
    )
    column_names = [col.column for col in virtual_table.columns]
    df_column_names = set(data_frame.columns)
    for col_name in column_names:
        if col_name not in df_column_names:
            data_frame[col_name] = None
            df_column_names.add(col_name)
    return data_frame[column_names]


def merge_query_results(results, query_doc, base_query_doc=None):
    include_data_source = False
    for col in (base_query_doc or query_doc).columns:
        if col.column == "data_source":
            include_data_source = True
            break

    first_columns = []
    df = pd.DataFrame()

    def is_lowercase_columns():
        return first_columns and first_columns[0]["label"].islower()

    for data_source, result in results:
        if not (result and result[0] and isinstance(result[0][0], dict)):
            continue
        columns = result[0]
        if not first_columns:
            first_columns = columns

        data = result[1:] if any(len(row) for row in result[1:]) else []
        df_columns = [col["label"] for col in columns]
        new_df = pd.DataFrame(data, columns=df_columns)
        if include_data_source:
            data_source_col = "data_source" if is_lowercase_columns() else "Data Source"
            new_df.insert(0, data_source_col, data_source)
        df = pd.concat([df, new_df], ignore_index=True)

    colnames = []
    df_cols = set(df.columns)
    added_col = False
    for row in query_doc.columns:
        colname = row.column if is_lowercase_columns() else row.label
        colnames.append(colname)
        if colname not in df_cols:
            df[colname] = None
            added_col = True
    if added_col:
        df = df[colnames]

    return [first_columns] + df.to_numpy().tolist()


def query_with_columns_in_table(query, data_source_name):
    """create a new query containing only the columns available in the data source"""
    query_str = """SELECT tc.column,t.table
        FROM `tabInsights Table Column`tc
        JOIN `tabInsights Table`t
            ON t.name = tc.parent
        WHERE t.table in %(tables)s
            AND t.data_source = %(data_source)s
            AND tc.column in %(col_names)s
    """

    tables = [row.table for row in query.tables]
    columns = [row.column for row in query.columns]
    if not (tables and columns):
        return query

    found = frappe.db.sql(
        query_str,
        {
            "data_source": data_source_name,
            "tables": tables,
            "col_names": columns,
        },
    )

    new_query = frappe.copy_doc(query)
    # remove columns not found in table for data source
    cols_found = set(found)
    new_query.columns = [
        row
        for row in new_query.columns
        if row.column != "data_source"
        and (row.aggregation or (row.column, row.table) in cols_found)
    ]
    return new_query


def validate_no_cycle_in_sources(data_source_doc):
    data_source = data_source_doc
    if isinstance(data_source_doc, str):
        if not frappe.db.exists("Insights Data Source", data_source_doc):
            return
        data_source = frappe.get_doc("Insights Data Source", data_source_doc)

    if not data_source.composite_datasource:
        return

    doc_tags = {tag for tag in (data_source.get("_user_tags") or "").strip().split(",")}
    sources_tags = {row.tag for row in data_source.get("sources")}

    for tag in doc_tags:
        if tag in sources_tags:
            frappe.throw(
                f"Tag '{tag}' cannot be applied to this document and "
                "be included in 'sources' field at the same time"
            )

    # check for nested composite data sources and get docnames
    _, cycle_source = get_nested_sources_for_virtual(data_source)
    if cycle_source:
        frappe.throw(f"Cycle detected in data sources: {frappe.as_json(cycle_source)}")


def compare(val1, condition, val2, fieldtype=None):
    # frappe.compare

    operator_map = {
        **frappe.utils.operator_map,
        "is": lambda a, b: bool(a) if b.lower() == "set" else not bool(a),
        "not_in": frappe.utils.operator_map["not in"],
        "starts_with": frappe.utils.operator_map["^"],
        "ends_with": lambda a, b: (a or "").endswith(b),
        "contains": lambda a, b: operator.contains(a, b),
        "not_contains": lambda a, b: not operator.contains(a, b),
    }

    ret = False
    if fieldtype:
        val1 = frappe.utils.cast(fieldtype, val1)
        val2 = frappe.utils.cast(fieldtype, val2)
    if condition in operator_map:
        ret = operator_map[condition](val1, val2)

    return ret


def apply_query_filters_for_datasource(sources, query):
    from insights.insights.doctype.insights_dashboard.utils import (
        convert_into_simple_filter,
    )

    filters = frappe.parse_json(query.filters) if (query and query.filters) else {}
    related = []
    for row in filters.get("conditions") or []:
        simple = convert_into_simple_filter(row)
        if not simple:
            continue
        if simple["column"]["column"] == "data_source":
            related.append(simple)

    if related:
        for source in sources:
            val = source
            if isinstance(source, frappe.model.document.Document):
                val = source.name
            for row in related:
                if compare(val, row["operator"], row["value"]):
                    yield source
    else:
        for x in sources:
            yield x


def remove_datasource_filters(query):
    if not query.filters:
        return query

    from insights.insights.doctype.insights_dashboard.utils import (
        convert_into_simple_filter,
    )

    filters = frappe.parse_json(query.filters)
    to_remove = []
    for row in filters.conditions[:]:
        simple = convert_into_simple_filter(row)
        if not simple:
            continue
        if simple["column"]["column"] == "data_source":
            to_remove.append(row)

    if to_remove:
        filters.conditions = [x for x in filters.conditions if x not in to_remove]
        new_query = frappe.copy_doc(query)
        new_query.filters = json.dumps(filters)
        return new_query
    return query
