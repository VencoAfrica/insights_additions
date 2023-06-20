frappe.ui.form.on("Insights Data Source", {
    refresh: function (frm) {
        frm.trigger("composite_datasource");
    },

    composite_datasource: function (frm) {
        [
            "database_type",
            "database_name",
            "username",
            "password",
            "host",
            "port",
            "use_ssl",
            "allow_imports",
        ].forEach((field) => {
            frm.set_df_property(field, "hidden", frm.doc.composite_datasource);
        });
    },
});
