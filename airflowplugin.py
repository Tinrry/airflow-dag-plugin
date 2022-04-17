
__author__ = "zhenghuanhuan"
__version__ = "2.2.5"

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
dag_creation_manager_bp = Blueprint(
    "dag_creation_manager_bp",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/test_plugin",
)




# # Creating a flask appbuilder BaseView
# class TestAppBuilderBaseNoMenuView(AppBuilderBaseView):
#     default_view = "test"
#
#     @expose("/")
#     def test(self):
#         return self.render_template("test_plugin/test.html", content="Hello galaxy!")

# v_appbuilder_nomenu_view = TestAppBuilderBaseNoMenuView()
# v_appbuilder_nomenu_package = {"view": v_appbuilder_nomenu_view}

# Creating flask appbuilder Menu Items
appbuilder_mitem = {
    "name": "Google",
    "href": "https://www.baidu.com",
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
        return "https://www.baidu.com"

from datetime import timedelta
import settings as dcmp_settings
from collections import OrderedDict

import airflow
from flask import request
from airflow.utils.db import provide_session

from models import DcmpDag
from utils import search_conf_iter

def get_current_user(raw=True):
    try:
        if raw:
            res = airflow.login.current_user.user
        else:
            res = airflow.login.current_user
    except Exception as e:
        res = None
    return res


def need_approver():
    if not dcmp_settings.AUTHENTICATE:
        return False
    if not dcmp_settings.DAG_CREATION_MANAGER_NEED_APPROVER:
        return False
    current_user = get_current_user(raw=False)
    if not current_user:
        return False
    if not hasattr(current_user, "is_approver"):
        return False
    return True

def can_access_approver():
    if not need_approver():
        return True
    current_user = get_current_user(raw=False)
    return not current_user.is_anonymous() and current_user.is_approver()

class RequestArgsFilter(object):
    def __init__(self, model, request_args, arg_tuple):
        self.model = model
        self.arg_tuple = arg_tuple
        self.request_args = request_args
        self.filter_arg_dict = None
        self.filter_groups = None
        self.active_filters = None
        self.filters = None
        self.filters_dict = {}
        self.refresh_filter_arg_dict()
        self.refresh_filter_groups()
        self.refresh_filters()

    def refresh_filter_arg_dict(self):
        filter_arg_dict = OrderedDict(self.arg_tuple)
        index = 0
        # for filter_arg, filter_params in filter_arg_dict.iteritems():
        for filter_arg, filter_params in filter_arg_dict.items():
            for filter_operation in filter_params["operations"]:
                filter_params["index"] = index
                index += 1
        self.filter_arg_dict = filter_arg_dict

    def refresh_filter_groups(self):
        filter_groups = OrderedDict()
        for filter_arg, filter_params in self.filter_arg_dict.items():
            for filter_operation in filter_params["operations"]:
                filter_groups[filter_arg] = [{
                    "index": filter_params["index"],
                    "operation": filter_operation,
                    "arg": "%s_%s" % (filter_arg, filter_operation),
                    "type": None,
                    "options": None
                }]
        self.filter_groups = filter_groups

    def refresh_filters(self):
        active_filters = []
        filters = []
        for key in sorted(self.request_args.keys()):
            value = self.request_args.get(key)
            if not value:
                continue
            if not key.startswith("flt"):
                continue
            key = key.split("_", 1)[-1]
            if key.find("_") == -1:
                continue
            arg, operation = key.rsplit("_")
            if arg not in self.filter_arg_dict:
                continue
            if operation not in self.filter_arg_dict[arg]["operations"]:
                continue
            active_filters.append([self.filter_arg_dict[arg]["index"], arg, value])
            if not self.filter_arg_dict[arg].get("no_filters"):
                filters.append(getattr(getattr(self.model, arg.lower().replace(" ", "_")), operation)(value))
            self.filters_dict[arg] = {"operation": operation, "value": value}
        self.active_filters = active_filters
        self.filters = filters



class DagCreationManager(AppBuilderBaseView):
    name = "dag"
    CONSTANT_KWS = {
        "TASK_TYPES": dcmp_settings.TASK_TYPES,
        "DAG_CREATION_MANAGER_LINE_INTERPOLATE": dcmp_settings.DAG_CREATION_MANAGER_LINE_INTERPOLATE,
        "DAG_CREATION_MANAGER_QUEUE_POOL": dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL,
        "DAG_CREATION_MANAGER_CATEGORYS": dcmp_settings.DAG_CREATION_MANAGER_CATEGORYS,
        "DAG_CREATION_MANAGER_TASK_CATEGORYS": dcmp_settings.DAG_CREATION_MANAGER_TASK_CATEGORYS,
    }
    # same variable not set here
    DEFAULT_CONF = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    default_view = 'index'
    @expose("/")
    @provide_session
    def index(self, session):
        TASK_NAME = "Task Name"
        COMMAND = "Command"
        # request_args_filter = RequestArgsFilter(DcmpDag, request.args, (
        #     ("Category", {"operations": ["contains"]}),
        #     (TASK_NAME, {"operations": ["contains"], "no_filters": True}),
        #     (COMMAND, {"operations": ["contains"], "no_filters": True}),
        # ))
        # confs = OrderedDict()
        # dcmp_dags = session.query(DcmpDag).order_by(DcmpDag.dag_name)
        # dcmp_dags_count = dcmp_dags.count()
        # dcmp_dags = dcmp_dags[:]
        # for dcmp_dag in dcmp_dags:
        #     dcmp_dag.conf = dcmp_dag.get_conf(session=session)
        #
        # if request_args_filter.filters_dict.get(TASK_NAME):
        #     task_name_value = request_args_filter.filters_dict.get(TASK_NAME)["value"]
        #
        #     def filter_dcmp_dags_by_task_name(dcmp_dag):
        #         for task in dcmp_dag.conf["tasks"]:
        #             if task_name_value in task["task_name"]:
        #                 return True
        #         return False
        #
        #     dcmp_dags = filter(filter_dcmp_dags_by_task_name, dcmp_dags)
        #
        # if request_args_filter.filters_dict.get(COMMAND):
        #     command_value = request_args_filter.filters_dict.get(COMMAND)["value"]
        #
        #     def filter_dcmp_dags_by_command(dcmp_dag):
        #         for task in dcmp_dag.conf["tasks"]:
        #             if command_value in task["command"]:
        #                 return True
        #         return False
        #
        #     dcmp_dags = filter(filter_dcmp_dags_by_command, dcmp_dags)
        # search = request.args.get("search", "")
        # if search:
        #     searched_dcmp_dags = []
        #     for dcmp_dag in dcmp_dags:
        #         dcmp_dag.search_results = []
        #         for result_task_name, result_key, result_line in search_conf_iter(search, dcmp_dag.conf):
        #             dcmp_dag.search_results.append({
        #                 "key": result_key,
        #                 "full_key": "%s__%s" % (result_task_name, result_key),
        #                 "line": result_line,
        #                 "html_line": (
        #                                  '<span class="nb">[%s]</span> ' % result_key if result_key else "") + result_line.replace(
        #                     search, '<span class="highlighted">%s</span>' % search),
        #             })
        #         if dcmp_dag.search_results:
        #             searched_dcmp_dags.append(dcmp_dag)
        #     dcmp_dags = searched_dcmp_dags
        #
        #
        # return self.render_template("test_plugin/test.html",
        #                    can_access_approver=can_access_approver(),
        #                    dcmp_dags=dcmp_dags,
        #                    dcmp_dags_count=dcmp_dags_count,
        #                    filter_groups=request_args_filter.filter_groups,
        #                    active_filters=request_args_filter.active_filters,
        #                    search=search, )

        return self.render_template("test_plugin/test.html")




dag_creation_manager_view = DagCreationManager()
v_appbuilder_package = {
    "name": "Test View",
    "category": "Test Plugin",
    "view": dag_creation_manager_view,
}

# Defining the plugin class
class DagCreationManagerPlugin(AirflowPlugin):
    name = "dag_creation_manager"
    hooks = [PluginHook]
    macros = [plugin_macro]
    flask_blueprints = [dag_creation_manager_bp]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = [appbuilder_mitem, appbuilder_mitem_toplevel]
    global_operator_extra_links = [
        GoogleLink(),
    ]
    operator_extra_links = [
    ]
