from pyparsing import basestring


def search_conf_iter(search, conf, key=None, task_name=""):
    if isinstance(conf, basestring):
        for line in conf.split("\n"):
            if search in line:
                yield task_name, key, line
    elif isinstance(conf, (tuple, list)):
        for item in conf:
            if isinstance(item, dict) and item.get("task_name"):
                task_name = item.get("task_name")
            for result_task_name, result_key, result_line in search_conf_iter(search, item, key=key, task_name=task_name):
                yield result_task_name, result_key, result_line
    elif isinstance(conf, dict):
        for item_key in sorted(conf.keys()):
            item_value = conf[item_key]
            for result_task_name, result_key, result_line in search_conf_iter(search, item_value, key=item_key, task_name=task_name):
                yield result_task_name, result_key, result_line
