from . import __version__ as app_version  # noqa: F401

app_name = "insights_additions"
app_title = "Insights Additions"
app_publisher = "Venco Ltd"
app_description = "Insights Additions"
app_email = "dev@venco.co"
app_license = "MIT"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/insights_additions/css/insights_additions.css"
# app_include_js = "/assets/insights_additions/js/insights_additions.js"

# include js, css files in header of web template
# web_include_css = "/assets/insights_additions/css/insights_additions.css"
# web_include_js = "/assets/insights_additions/js/insights_additions.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "insights_additions/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
doctype_js = {"Insights Data Source": "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "insights_additions.utils.jinja_methods",
# 	"filters": "insights_additions.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "insights_additions.install.before_install"
# after_install = "insights_additions.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "insights_additions.uninstall.before_uninstall"
# after_uninstall = "insights_additions.uninstall.after_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "insights_additions.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }
override_doctype_class = {
    "Insights Table": "insights_additions.overrides.doctype.CustomInsightsTable",
    "Insights Data Source": "insights_additions.overrides.doctype.CustomInsightsDataSource",
    "Insights Query": "insights_additions.overrides.doctype.CustomInsightsQuery",
}

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"insights_additions.tasks.all"
# 	],
# 	"daily": [
# 		"insights_additions.tasks.daily"
# 	],
# 	"hourly": [
# 		"insights_additions.tasks.hourly"
# 	],
# 	"weekly": [
# 		"insights_additions.tasks.weekly"
# 	],
# 	"monthly": [
# 		"insights_additions.tasks.monthly"
# 	],
# }

# Testing
# -------

# before_tests = "insights_additions.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "insights_additions.event.get_events"
# }
override_whitelisted_methods = {
    "insights.api.get_tables": "insights_additions.overrides.functions.get_tables",
    "insights.api.get_table_name": "insights_additions.overrides.functions.get_table_name",
    "frappe.desk.doctype.tag.tag.add_tag": "insights_additions.overrides.functions.add_tag",
    "insights.insights.doctype.insights_dashboard.insights_dashboard.get_queries_column": "insights_additions.overrides.functions.get_queries_column",
}
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "insights_additions.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["insights_additions.utils.before_request"]
# after_request = ["insights_additions.utils.after_request"]

# Job Events
# ----------
# before_job = ["insights_additions.utils.before_job"]
# after_job = ["insights_additions.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"insights_additions.auth.validate"
# ]
