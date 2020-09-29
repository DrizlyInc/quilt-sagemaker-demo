from logging.config import dictConfig
#TODO: Make this a config or an arg
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    }
})
#TODO: Certain info logs should be written to file instead of to the stream.
#TODO: Really the goal is just to get a sense of where errors are occurring
#TODO: That said if the data service gets written properly, perhaps the verbosity won't be a prblem
from flask import Flask, Response, request
import pandas as pd
import json
import sys
import traceback



#TODO: get redis host here
app = Flask(__name__)
app.logger.info(f"Loaded Flask App Named:{__name__}")
from app.eta import adapter

#Load connfiguration
#TODO: Make this play nice with docker-compose for local development
import json
import os
with open('/app/eta/config/config.json', 'r') as f:
    config = json.load(f)

env = os.environ.get('ETAS_ENV')
model = adapter.Adapter(redis_host=config[env]["redis"]["host"])
app.logger.info("Loaded Adapter")

def run_model(request):
    """Predictor function."""
    app.logger.debug(f"Predict request:{request}")
    return model.predict(request)


@app.route('/ping', methods=['GET'])
def ping():
    """
    Determine if the container is healthy by running a sample through the algorithm.
    """
    return Response(response='{"status": "ok"}', status=200, mimetype='application/json')

# def ping():
#     """
#     Determine if the container is healthy by running a sample through the algorithm.
#     """
#     # we will return status ok if the model doesn't barf
#     # but you can also insert slightly more sophisticated tests here
#     if model._status =="LOADING":
#         return Response(response='{"status": "loading"}', status=500, mimetype='application/json')
#     health_check_request_array = pd.read_csv("/opt/ml/model/health-check-data.csv").to_dict('records')
#     health_check_request_dict = {"eta_requests": health_check_request_array}
#     app.logger.debug(f"Ping request with health check array:{health_check_request_dict}")
#     try:
#         result = run_model(health_check_request_dict)
#         return Response(response='{"status": "ok"}', status=200, mimetype='application/json')
#     except Exception as inst:
#         #TODO: Change this to just log the tope of error in the response
#         e = sys.exc_info()
#         print(" ".join(traceback.format_exception(*e)), flush=True)
#         response = {"status": "error", "type": str(e[0]), "value": str(e[1])}
#         return Response(response=json.dumps(response), status=500, mimetype='application/json')

# Post is of form {"eta_requests": Array of Json requests, ...}
# an individual request should take the form of a dictionary {"store_id", "delivery_zipcode",
@app.route('/invocations', methods=['POST'])
def predict():
    """
    Do an inference on a single batch of data.
    """
    app.logger.info(f"request:{request.get_data()}")
    results = run_model(request.get_json())
    app.logger.info(f"response:{results}")
    return Response(response=json.dumps(results), status=200, mimetype='application/json')

# Just a ping repeated 100 times internally
@app.route('/performance_test', methods=['GET'])
def performance_test():
    """
    Determine if the container is healthy by running a sample through the algorithm.
    """
    # we will return status ok if the model doesn't barf
    # but you can also insert slightly more sophisticated tests here
    if model._status =="LOADING":
        return Response(response='{"status": "loading"}', status=500, mimetype='application/json')
    health_check_request_array = list()
    for i in range(100):
        health_check_request_array += pd.read_csv("/opt/ml/model/health-check-data.csv").to_dict('records')
    health_check_request_dict = {"eta_requests": health_check_request_array}
    app.logger.debug(f"Ping request with health check array:{health_check_request_dict}")
    try:
        import time
        start = time.process_time()
        request_time = run_model(health_check_request_dict)
        end = time.process_time()
        return Response(response=f"time:{end-start}", status=200, mimetype='application/json')
    except Exception as inst:
        #TODO: Change this to just log the tope of error in the response
        e = sys.exc_info()
        print(" ".join(traceback.format_exception(*e)), flush=True)
        response = {"status": "error", "type": str(e[0]), "value": str(e[1])}
        return Response(response=json.dumps(response), status=500, mimetype='application/json')
#TODO: Expose endpoint permitting additional predictions about stage of the order.