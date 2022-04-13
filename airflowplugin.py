# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

# Importing base classes that we need to derive
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperatorLink

# Will show up in Connections screen in a future version
class PluginHook(BaseHook):
    pass


# Will show up under airflow.macros.test_plugin.plugin_macro
# and in templates through {{ macros.test_plugin.plugin_macro }}
def plugin_macro():
    pass


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test_plugin",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/test_plugin",
)

# Creating a flask appbuilder BaseView
class TestAppBuilderBaseView(AppBuilderBaseView):
    default_view = "test"

    @expose("/")
    def test(self):
        return self.render_template("test_plugin/test.html", content="Hello galaxy!")


# Creating a flask appbuilder BaseView
class TestAppBuilderBaseNoMenuView(AppBuilderBaseView):
    default_view = "test"

    @expose("/")
    def test(self):
        return self.render_template("test_plugin/test.html", content="Hello galaxy!")


v_appbuilder_view = TestAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Test View",
    "category": "Test Plugin",
    "view": v_appbuilder_view,
}

v_appbuilder_nomenu_view = TestAppBuilderBaseNoMenuView()
v_appbuilder_nomenu_package = {"view": v_appbuilder_nomenu_view}

# Creating flask appbuilder Menu Items
appbuilder_mitem = {
    "name": "Google",
    "href": "https://www.google.com",
    "category": "Search",
}
appbuilder_mitem_toplevel = {
    "name": "Apache",
    "href": "https://www.apache.org/",
}

# A global operator extra link that redirect you to
# task logs stored in S3
class GoogleLink(BaseOperatorLink):
    name = "Google"

    def get_link(self, operator, dttm):
        return "https://www.google.com"



# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    hooks = [PluginHook]
    macros = [plugin_macro]
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package, v_appbuilder_nomenu_package]
    appbuilder_menu_items = [appbuilder_mitem, appbuilder_mitem_toplevel]
    global_operator_extra_links = [
        GoogleLink(),
    ]
    operator_extra_links = [
    ]
