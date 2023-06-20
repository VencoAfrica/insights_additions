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


def get_sources_for_virtual(data_source, get_docs=True, as_generator=False):
    if isinstance(data_source, str):
        data_source = frappe.get_doc("Insights Data Source", data_source)

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


def get_columns_for_virtual_table(virtual_table, get_docs=True):
    if isinstance(virtual_table, str):
        virtual_table = frappe.get_doc("Insights Table", virtual_table)
    data_source = frappe.get_doc("Insights Data Source", virtual_table.virtual_data_source)

    columns = virtual_table.get("columns")
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
        assert result and result[0] and isinstance(result[0][0], dict)
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
        if (row.column, row.table) in cols_found and row.column != "data_source"
    ]
    return new_query
