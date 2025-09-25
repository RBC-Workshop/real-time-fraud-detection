
# Import functions and objects the microservice needs.
# - Flask is the top-level application. You implement the application by adding methods to it.
# - Response enables creating well-formed HTTP/REST responses.
# - requests enables accessing the elements of an incoming HTTP/REST request.
#

import json
import copy
import pandas as pd
from datetime import datetime
import os
import uuid

from flask import Flask, Response, render_template, jsonify
from flask import request
from flask_table import Table, Col
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.cassandra import CassandraInstrumentor

from resources import query_execute as C

_default_limit = 10


application = Flask(__name__)

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTLP_ENDPOINT", "http://localhost:4317"),
    insecure=True,
)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

FlaskInstrumentor().instrument_app(application)
CassandraInstrumentor().instrument()

request_count = Counter('flask_requests_total', 'Total Flask requests', ['method', 'endpoint'])
request_duration = Histogram('flask_request_duration_seconds', 'Flask request duration')
cassandra_query_duration = Histogram('cassandra_query_duration_seconds', 'Cassandra query duration')
active_connections = Gauge('cassandra_active_connections', 'Active Cassandra connections')


##################################################################################################################

def _get_and_remove_arg(args, arg_name):

    val = copy.copy(args.get(arg_name, None))
    if val is not None:
        del args[arg_name]

    return args, val


def _de_array_args(args):

    result = {}

    if args is not None:
        for k,v in args.items():
            result[k] = ",".join(v)

    return result


# 1. Extract the input information from the requests object.
# 2. Log the information
# 3. Return extracted information.
#
def log_and_extract_input(method, path_params=None):

    path = request.path
    args = dict(request.args)
    args = _de_array_args(args)
    data = None
    headers = dict(request.headers)
    method = request.method

    args, limit = _get_and_remove_arg(args, "limit")
    args, offset = _get_and_remove_arg(args, "offset")
    args, order_by = _get_and_remove_arg(args, "order_by")
    args, fields = _get_and_remove_arg(args, "fields")

    args = _de_array_args(args)

    if limit is None:
        limit = _default_limit

    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        data = "You sent something but I could not get JSON out of it."

    log_message = str(datetime.now()) + ": Method " + method

    inputs =  {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data,
        "limit": limit,
        "offset": offset,
        "order_by": order_by,
        "url": request.url,
        "base_url": request.base_url,
        "fields": fields
        }

    log_message += " received: \n" + json.dumps(inputs, indent=2)
    print(log_message)

    return inputs


def log_response(method, status, data, txt):

    msg = {
        "method": method,
        "status": status,
        "txt": txt,
        "data": data
    }

    print(str(datetime.now()) + ": \n" + json.dumps(msg, indent=2, default=str))


# This function performs a basic health check. We will flesh this out.
@application.route("/api/health", methods=["GET"])
def health_check():
    with tracer.start_as_current_span("health_check") as span:
        request_count.labels(method='GET', endpoint='/api/health').inc()
        with request_duration.time():
            span.set_attribute("http.method", "GET")
            span.set_attribute("http.route", "/api/health")
            rsp_data = { "status": "healthy", "time": str(datetime.now()) }
            return jsonify(rsp_data)


@application.route("/api/demo/<parameter>", methods=["GET", "POST"])
def demo(parameter):

    inputs = log_and_extract_input(demo, { "parameter": parameter })

    msg = {
        "/demo received the following inputs" : inputs
    }

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


@application.route("/api/customer/<cc_num>", methods=["GET"])
def get_character_by_id(cc_num):
    with tracer.start_as_current_span("get_customer") as span:
        request_count.labels(method='GET', endpoint='/api/customer').inc()
        with request_duration.time():
            with cassandra_query_duration.time():
                res = C.get_costumer_by_id(cc_num)
            span.set_attribute("http.method", "GET")
            span.set_attribute("http.route", "/api/customer")
            span.set_attribute("customer.cc_num", cc_num)
            df = pd.DataFrame([res], columns=list(res.keys()))
            return render_template("customer.html", tables=[df.to_html(index=False)], titles=df.columns.values, cc_num=cc_num)

    # rsp = Response(json.dumps(res), status=200, content_type="application/json")
    # return rsp


@application.route("/api/statement/<cc_num>", methods=["GET"])
def get_statement_by_id(cc_num):
    with tracer.start_as_current_span("get_statement") as span:
        request_count.labels(method='GET', endpoint='/api/statement').inc()
        with request_duration.time():
            with cassandra_query_duration.time():
                res = C.get_statement_by_id(cc_num)
            span.set_attribute("http.method", "GET")
            span.set_attribute("http.route", "/api/statement")
            span.set_attribute("customer.cc_num", cc_num)
            span.set_attribute("statement.transaction_count", len(res["data"]))
            df = pd.DataFrame(res["data"])
            df.sort_values(by="trans_time")
            return render_template("statement.html", tables=[df.to_html(index=False)], titles=df.columns.values, cc_num=cc_num)

    # rsp = Response(json.dumps(res), status=200, content_type="application/json")
    # return rsp


@application.route("/metrics")
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    
    port = int(os.getenv("FLASK_PORT", "5050"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    application.run(host='0.0.0.0', port=port, debug=debug)
