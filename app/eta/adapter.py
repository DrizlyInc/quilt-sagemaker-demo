## This will staple all the models together along with the data service and expose a predict function
import logging
import traceback

logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger("app.adapter")
# For debug purposes
import sys
import json
sys.path.append("..")
from app.eta.util.DataServiceConnector import DataService
from joblib import load
from werkzeug.exceptions import BadRequest

class Adapter:
    def __init__(
        self, redis_host="redis", model_filename="/opt/ml/model/composed_model.pkl"
    ):
        self._status = "LOADING"
        logging.info(f"Loading data service:{redis_host}")
        self._data_service = self.load_data_service(redis_host)
        logging.info("Loaded data service")
        logging.info(f"Loading model:{model_filename}")
        # TODO: Might be better to put all this logic in a model of models class
        self._model = self.load_model(model_filename)
        logging.info("READY")
        self._status = "READY"

    # Sagemaker requires model files to be written to /opt/ml/model. I assume during deploy they untar to the same dir
    def load_model(self, model_filename):
        return load(model_filename)

    def load_data_service(self, host):
        data_service = DataService()
        data_service.set_redis_client(host=host, port=6379, db=0)
        return data_service

    def predict(self, request):
        """Predict the label of a set of strings
        strings -- array of strings to classify
        """
        # Should be a json array
        eta_responses = {}
        # To permit better diagnostics during service failures
        try:
            logging.info(f"Processing request {request}")
            request_json = json.loads(request)
            if len(request_json["eta_requests"]) < 1:
                logging.error(f"Bad request:{request}")
                raise BadRequest("eta_requests array contains no values")
        except (TypeError, json.decoder.JSONDecodeError) as e:
            raise BadRequest(f"Request:{request} is not valid JSON. {e}")
        except KeyError as e:
            raise BadRequest(f"There is no key 'eta_requests' present. {e}")
        except Exception as e:
            logging.error(f"Error {e} for request {request}")
            raise e
        # Get number of drivers for the store in the requests
        # This will return a list of requests equal number of active drivers
        for eta_request_dict in request_json["eta_requests"]:
            # The intent here is to throw an error whenever the store_id or delivery_zipcode is missing
            # and extract only the relevant fields for prediction for the get_data call
            try:
                store_id = eta_request_dict["store_id"]
                delivery_zipcode = eta_request_dict["delivery_zipcode"]
            except KeyError as e:
                logging.error(
                    f"One of the requests is missing a critical key. store_id and delivery_zipcode are needed to render any ETA. If store_id is missing the responses cannot be formatted properly so an error must be thrown")
                raise BadRequest(f"A request threw a critical key error {e}. Requests need 'store_id' amd 'delivery_zipcode' keys present for proper responses")
            try:
                this_request_dict = {
                 "store_id": store_id,
                    "delivery_zipcode": delivery_zipcode
                }
                eta_request_arr = self._data_service.get_data(
                    this_request_dict
                )
                model_prediction = self.predict_individual_request(eta_request_arr)
                logging.info(f"PREDICTION: {eta_request_dict['store_id']}:{model_prediction}")
            # Two error types:
            # ValueError refers to an error with what gets input into the model
            # A nan is the most common, but any bad value in a column in the dataframe added to eta_request_arr
            # TypeError refers to a failure to get something from redis, and then subsequently attempting to load
            # it as a dictionary or otherwise perform operations on this.
            # Both of these errors are expected to occur in some percentage of the time, as the aggregate values
            # That the data service pulls are defined by another application that can (but shouldn't) have gaps
            #TODO: Shrink this
            except (TypeError, ValueError, KeyError) as e:
                tb = sys.exc_info()[-1]
                stk = traceback.extract_tb(tb)
                fname = stk[-1][-1]
                logging.exception(e)
                logging.info("Using fallback model")
                model_prediction = self._data_service.get_eta_fallback(eta_request_dict)
                logging.info(
                    f"Fallback model for:{eta_request_dict} because there was a returned an error {e} '{fname}' fallback_predictions:{model_prediction}"
                )
            except Exception as e:
                logging.error("Unknown exception")
                logging.exception(e)
                model_prediction = {"response_code":500, "error_message": e}
            eta_responses.update({eta_request_dict["store_id"]: model_prediction})
        return eta_responses

    # delivery_request_dict_with_data = [{"store_metadata": store_metadata,
    #                                "store_history": store_history,
    #                                "store_context": store_context,
    #                                "store_history_live": store_history_live,
    #                                "driver_history": driver_history}]
    # TODO: If responses are slow, consider rewriting for batch processing. One big dataframe and a single predict call
    # will be faster than many dataframes and many predict calls.
    def predict_individual_request(self, eta_request_arr):
        # These values are for averaging results
        count = 0.0
        lower_bound_sum = 0.0
        upper_bound_sum = 0.0
        # Compute an average for every active driver a store has
        # This should actually be a request of a single dataframe
        # The downside is that errors will kill the whole thing
        for eta_request_df in eta_request_arr:
            # logging.debug(f"{eta_request_df.to_dict('records')}")
            lower_bound, upper_bound = self._model.predict(eta_request_df)
            logging.debug(f"prediction:{lower_bound},{upper_bound}")
            lower_bound_sum += lower_bound
            upper_bound_sum += upper_bound
            count += 1.0
        return {"lower":lower_bound_sum / count, "upper":upper_bound_sum / count, "response_code": 200}


if __name__ == "__main__":
    # requires redis to be running at localhost
    import cProfile

    adapter = Adapter(redis_host="eta-redis.dgvvsn.0001.use1.cache.amazonaws.com")
                      #model_filename="data/composed_model.pkl")
    # This store id should throw an error as it won't be in any of the fall back logic
    eta_request = {
        "eta_requests": [
            {"store_id": -1, "delivery_zipcode": 11550, "hod": 17, "dow": "Fri"}
        ]
    }
    adapter.predict(eta_request)
    # For timing
    # cProfile.run("adapter.predict(eta_request, hod=17, dow='Fri')", sort='cumulative')
