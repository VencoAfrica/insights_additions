import frappe


def make_virtual_table_name(table_name, virtual_data_source_name):
    return f"{table_name}::{virtual_data_source_name}"


def split_virtual_table_name(virtual_table_name):
    if not virtual_table_name:
        return ("", "")

    parts = tuple(virtual_table_name.rsplit("::", 1))
    if len(parts) < 2:
        return parts[0], ""
    return parts


def get_sources_for_virtual(data_source, get_docs=True):
    if isinstance(data_source, str):
        data_source = frappe.get_doc("Insights Data Source", data_source)

    tags = [row.tag for row in data_source.get("sources")]
    sources = [
        (frappe.get_doc("Insights Data Source", r[0]) if get_docs else r[0])
        for r in frappe.get_list(
            "Tag Link",
            filters={"tag": ["in", tags], "document_type": "Insights Data Source"},
            fields=["document_name"],
            as_list=1,
        )
    ]

    return sources


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

    virtual_table.append(
        "columns",
        {"name": "*" * 10, "column": "data_source", "label": "Data Source", "type": "String"},
    )
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
