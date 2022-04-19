from datetime import datetime
from datetime import timedelta
import json


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, timedelta):
            return o.total_seconds()

        return json.JSONEncoder.default(self, o)
# import flask
# flask.json.dumps