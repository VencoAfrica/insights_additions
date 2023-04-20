import frappe


def get_sources_for_virtual(data_source, get_docs=True):
    if isinstance(data_source, str):
        data_source = frappe.get_doc("Insights Data Source", data_source)

    sources = []
    for name in data_source.get_tags():
        if docname := frappe.db.exists("Insights Data Source", name):
            sources.append(frappe.get_doc("Insights Data Source", docname) if get_docs else name)

    return sources
