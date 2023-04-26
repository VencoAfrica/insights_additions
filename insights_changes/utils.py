import frappe


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
