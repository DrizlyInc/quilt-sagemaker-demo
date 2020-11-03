import redis
import json
import datetime
import pytz
import pandas as pd
import math

import logging
from distutils.util import strtobool
from werkzeug.exceptions import BadRequest
# logger = logging.getlogger()

# class InvalidUsage(werkzeug.exceptions.BadRequest):
#     status_code = 400
#
#     def __init__(self, message, status_code=None, payload=None):
#         Exception.__init__(self)
#         self.message = message
#         if status_code is not None:
#             self.status_code = status_code
#         self.payload = payload
#
#     def to_dict(self):
#         rv = dict(self.payload or ())
#         rv['message'] = self.message
#         return rv

class DataService:
    def __init__(self):
        self._redis_client = None

    def set_redis_client(self, host="redis", port=6379, db=0):
        logging.info(f"Connecting to redis client at: {host}:{port}")
        self._redis_client = redis.StrictRedis(
            host=host, port=port, db=db, charset="utf-8", decode_responses=True
        )

    def strtoarr(self, string):
        if "[" in string and "]" in string:
            return json.loads(string)
        else:
            return None

    def betterstrtobool(self, string):
        try:
            return bool(strtobool(string))
        except ValueError as e:
            logging.debug(f"ValueError in betterstrtobool")
            return False

    def betterstrtofloat(self, string):
        try:
            return float(string)
        except:
            logging.debug(f"Unknown String:{string}")
            return string

    def transform_dict(self, dict_to_convert, type_dict):
        converted_dict = dict()
        for k, v in dict_to_convert.items():
            # If it is a specified transformation, apply it, else treat as float
            try:
                transform_function = type_dict.get(
                    k, self.betterstrtofloat
                )
                converted_dict[k] = transform_function(v)
            except Exception as e:
                logging.error(
                    f"Error of type {e.type} in redis transform for key:{k} with value: {v} with transform function: {transform_function}"
                )
                raise BadRequest(f"Invalid input type for the field:{k} with the value: {v} could not be converted to type: {transform_function}")
        return converted_dict

    # redis likes strings. sklearn does not.
    def transform_redis_data_to_correct_type(self, redis_hashmap:dict) -> dict:
        transformation_function_dict = {
            "store_id" : int,
            "driver_id" : int,
            "is_a_driver_at_store": self.betterstrtobool,
            "available_driver_ids": self.strtoarr,
            "marking_enroute_poorly": self.betterstrtobool,
            "time_zone": str,
            "dow": str,
            "store_zipcode": str,
            "delivery_zipcode": str,
            "mx_date": str,
            "delivery_state": str,
            "training_data_date":str
        }
        try:
            transformed_redis_data = self.transform_dict(redis_hashmap, transformation_function_dict)
        except Exception as e:
            #Exception already logged. This shouldn't block the job from returning a default value
            transformed_redis_data = dict()
        return transformed_redis_data

    def get_from_redis(self, key : str) -> dict:
        redis_hashmap = self._redis_client.hgetall(key)
        logging.debug(f"Got key:{key} Value:{redis_hashmap}")
        transformed_redis_data = self.transform_redis_data_to_correct_type(
            redis_hashmap
        )
        logging.debug(f"Transformed:{key} Value:{transformed_redis_data}")
        return transformed_redis_data

    # "store_id",
    # "active_on_demand_orders",
    # "active_scheduled_orders",
    # "total_orders_preceding",
    # "stores_active_drivers",
    # "drivers_still_out_ct",
    # "drivers_returning_ct",
    # "is_a_driver_at_store",
    # "available_driver_ids",
    def get_store_context(self, request_dict):
        namespace = "store_context"
        namespace_id = request_dict["store_id"]
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # store_id
    # [driver_ids]
    def get_active_driver_ids(self, request_dict):
        namespace = "active_drivers"
        namespace_id = request_dict["store_id"]
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # store_id
    # store_zipcode
    # state
    # timezone
    def get_store_metadata(self, request_dict):
        namespace = "store_metadata"
        namespace_id = request_dict["store_id"]
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # "store_id",
    # "minutes_placed_to_accepted_median",
    # "minutes_accepted_to_enroute_median",
    # "minutes_enroute_to_complete_median",
    # "delivery_minutes_median",
    # "minutes_placed_to_accepted_median_week_diff",
    # "minutes_accepted_to_enroute_median_week_diff",
    # "minutes_enroute_to_complete_median_week_diff",
    # "delivery_minutes_median_week_diff",
    # "minutes_enroute_to_complete_stddev_hour",
    # "minutes_enroute_to_complete_stddev_day",
    # "minutes_enroute_to_complete_stddev_week",
    # "minutes_placed_to_accepted_stddev_hour",
    # "minutes_placed_to_accepted_stddev_day",
    # "minutes_placed_to_accepted_stddev_week",
    # "delivery_minutes_stddev_hour",
    # "delivery_minutes_stddev_day",
    # "delivery_minutes_stddev_week",
    # "orders_per_driver_median",
    # "marking_enroute_poorly",
    # If this fails. The service cannot render a request
    def get_store_order_history(self, request_dict):
        namespace = "store_order_history"
        namespace_id = f"{request_dict['store_id']}"
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # "delivery_zipcode".
    # "dow",
    # "hod",
    # "enroute_to_complete_zipwide_extra_median_delta",
    # "enroute_to_complete_zipwide_intra_median_delta",
    # "enroute_to_complete_zipwide_dow_extra_median_delta",
    # "enroute_to_complete_zipwide_dow_intra_median_delta",
    # "enroute_to_complete_zipwide_hod_extra_median_delta",
    # "enroute_to_complete_zipwide_hod_intra_median_delta",
    # "enroute_to_complete_zipwide_dow_hod_extra_median_delta",
    # "enroute_to_complete_zipwide_dow_hod_intra_median_delta",
    # "enroute_to_complete_zipwide_waterfall",
    def get_location_time_deltas(self, request_dict):
        namespace = "location_time_deltas"
        namespace_id = f"{request_dict['delivery_zipcode']},{request_dict['dow']},{request_dict['hod']}"
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # store_id
    # driver_id
    # driver_minutes_placed_to_accepted_diff,
    # driver_minutes_accepted_to_enroute_diff,
    # driver_enroute_to_complete_diff,
    # driver_delivery_minutes_diff,
    # TODO: Handle missing values. If spark is missing one of the fields it will not write the column
    def get_driver_history(self, request_dict):
        namespace = "driver_history"
        namespace_id = f"{request_dict['store_id']},{request_dict['driver_id']}"
        return self.get_from_redis(f"{namespace}:{namespace_id}")


    # -- Daily diffs
    # store_order_history.minutes_placed_to_accepted_median_day_diff,
    # store_order_history.minutes_accepted_to_enroute_median_day_diff,
    # store_order_history.minutes_enroute_to_complete_median_day_diff,
    # store_order_history.delivery_minutes_median_day_diff,
    #   -- Hourly diffs
    # store_order_history.minutes_placed_to_accepted_median_hour_diff,
    # store_order_history.minutes_accepted_to_enroute_median_hour_diff,
    # store_order_history.minutes_enroute_to_complete_median_hour_diff,
    # store_order_history.delivery_minutes_median_hour_diff,
    #   -- Last 3 order diffs
    # store_order_history.minutes_placed_to_accepted_wsum_last_n_completed_orders_diff,
    # store_order_history.minutes_accepted_to_enroute_wsum_last_n_completed_orders_diff,
    # store_order_history.minutes_enroute_to_complete_wsum_last_n_completed_orders_diff,
    # store_order_history.delivery_minutes_wsum_last_n_completed_orders_diff,
    # minutes_enroute_to_complete_stddev_hour,
    # minutes_enroute_to_complete_stddev_day,
    # minutes_placed_to_accepted_stddev_hour,
    # minutes_placed_to_accepted_stddev_day,
    # delivery_minutes_stddev_hour,
    # delivery_minutes_stddev_day
    def get_store_history_live(self, request_dict):
        namespace = "store_history_live"
        namespace_id = request_dict["store_id"]
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    def get_local_hod_dow(self, timezone):
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        timezone = pytz.timezone(timezone)
        local_time = timezone.normalize(now_utc.astimezone(timezone))
        hod = local_time.hour
        # dow as a string rather than an int
        dow = local_time.strftime("%a")
        logging.debug(f"Current dow:{dow} hod:{hod}")
        return hod, dow

    def fill_in_store_context_nulls(self, redis_data):
        if len(redis_data["store_context"]) < 1:
            missing_values_filled = {
                "active_on_demand_orders": 0.0,
                "active_scheduled_orders": 0.0,
                "total_orders_preceding": 0.0,
                "stores_active_drivers": 0.0,
                "drivers_still_out_ct": 0.0,
                "drivers_returning_ct": 0.0,
                "is_a_driver_at_store": False,
                "available_driver_ids": [],
            }
        else:
            missing_values_filled = {}
            for k, v in redis_data["store_context"].items():
                if v is None or (
                    k not in ["available_driver_ids", "is_a_driver_at_store"]
                    and math.isnan(v)
                ):
                    if k == "is_a_driver_at_store":
                        val = False
                    else:
                        val = 0.0
                else:
                    val = v
                missing_values_filled[k] = val
        redis_data["store_context"] = missing_values_filled
        return redis_data

    def fill_in_driver_history_nulls(self, redis_data):
        no_driver_history_dict = {
            "driver_minutes_placed_to_accepted_diff": 0.0,
            "driver_minutes_accepted_to_enroute_diff": 0.0,
            "driver_enroute_to_complete_diff": 0.0,
            "driver_delivery_minutes_diff": 0.0,
        }
        redis_data["driver_history"] = [no_driver_history_dict]
        return redis_data

    def fill_in_store_history_nulls(self, redis_data):
        missing_values_filled = {
            "minutes_placed_to_accepted_median_day_diff": 0.0,
            "minutes_accepted_to_enroute_median_day_diff": 0.0,
            "minutes_enroute_to_complete_median_day_diff": 0.0,
            "delivery_minutes_median_day_diff": 0.0,
            "minutes_placed_to_accepted_median_hour_diff": 0.0,
            "minutes_accepted_to_enroute_median_hour_diff": 0.0,
            "minutes_enroute_to_complete_median_hour_diff": 0.0,
            "delivery_minutes_median_hour_diff": 0.0,
            "minutes_placed_to_accepted_wsum_last_n_completed_orders_diff": 0.0,
            "minutes_accepted_to_enroute_wsum_last_n_completed_orders_diff": 0.0,
            "minutes_enroute_to_complete_wsum_last_n_completed_orders_diff": 0.0,
            "delivery_minutes_wsum_last_n_completed_orders_diff": 0.0,
            "minutes_enroute_to_complete_stddev_hour": 0.0,
            "minutes_enroute_to_complete_stddev_day": 0.0,
            "minutes_placed_to_accepted_stddev_hour": 0.0,
            "minutes_placed_to_accepted_stddev_day": 0.0,
            "delivery_minutes_stddev_hour": 0.0,
            "delivery_minutes_stddev_day": 0.0,
        }

        if len(redis_data["store_history_live"]) > 0:
            missing_values_filled = {
                k: (
                    v
                    if redis_data.get(k) is not None
                    and not math.isnan(redis_data[k])
                    else 0.0
                )
                for k, v in missing_values_filled.items()
            }
            # missing_values_filled = {k: (v if not math.isnan(v) else 0.0) for k, v in redis_data["store_history_live"].items()}
        redis_data["store_history_live"] = missing_values_filled
        return redis_data

    def calculate_waterfall(self, target_value, redis_data):
        store_order_history_dict = redis_data[
            "store_order_history"
        ]
        store_history_live = redis_data["store_history_live"]
        median = store_order_history_dict[f"{target_value}_median"]
        stddev_hour = store_history_live[f"{target_value}_stddev_hour"]
        stddev_day = store_history_live[f"{target_value}_stddev_day"]
        stddev_week = store_order_history_dict[f"{target_value}_stddev_week"]
        wsum_last_n_completed_orders_diff = store_history_live[
            f"{target_value}_wsum_last_n_completed_orders_diff"
        ]
        median_hour_diff = store_history_live[f"{target_value}_median_hour_diff"]
        median_day_diff = store_history_live[f"{target_value}_median_day_diff"]
        median_week_diff = store_order_history_dict[f"{target_value}_median_week_diff"]
        adding_value = median_week_diff
        if (
            median_day_diff - 1.28 * stddev_day
            <= wsum_last_n_completed_orders_diff
            <= median_day_diff + 1.28 * stddev_day
            and median_hour_diff - 1.28 * stddev_hour
            <= wsum_last_n_completed_orders_diff
            <= median_hour_diff + 1.28 * stddev_hour
        ):
            adding_value = wsum_last_n_completed_orders_diff
        elif (
            median_hour_diff - 1.28 * stddev_day
            <= median_hour_diff
            <= median_hour_diff + 1.28 * stddev_day
        ):
            adding_value = median_hour_diff
        elif (
            median_week_diff - 1.28 * stddev_week
            <= median_day_diff
            <= median_week_diff + 1.28 * stddev_week
        ):
            adding_value = median_day_diff
        waterfall = median + adding_value
        logging.debug(f"Waterfall for:{target_value} = {waterfall}")
        return {f"{target_value}_waterfall": waterfall}

    def calculate_location_waterfall(
        self, redis_data, store_metadata, location_time_deltas
    ):
        intra_zip_keys = [
            "enroute_to_complete_zipwide_dow_hod_intra_median_delta",
            "enroute_to_complete_zipwide_hod_intra_median_delta",
            "enroute_to_complete_zipwide_dow_intra_median_delta",
            "enroute_to_complete_zipwide_intra_median_delta",
        ]
        extra_zip_keys = [
            "enroute_to_complete_zipwide_dow_hod_extra_median_delta",
            "enroute_to_complete_zipwide_hod_extra_median_delta",
            "enroute_to_complete_zipwide_dow_extra_median_delta",
            "enroute_to_complete_zipwide_extra_median_delta",
        ]
        val = 0.0
        # TODO: Might have to do some lower/upper case on delivery_zipcode here depending on API
        if (
            store_metadata["store_zipcode"] == redis_data["delivery_zipcode"]
        ):
            for intra_zip_key in intra_zip_keys:
                if location_time_deltas.get(
                    intra_zip_key
                ) is not None and not math.isnan(location_time_deltas[intra_zip_key]):
                    val = location_time_deltas[intra_zip_key]
        else:
            for extra_zip_key in extra_zip_keys:
                if location_time_deltas.get(
                    extra_zip_key
                ) is not None and not math.isnan(location_time_deltas[extra_zip_key]):
                    val = location_time_deltas[extra_zip_key]
        return {"enroute_to_complete_zipwide_waterfall": val}

    def calculate_most_granular(self, target_value, redis_data):
        store_order_history_vals = redis_data[
            "store_order_history"
        ]
        store_history_live_vals = redis_data["store_history_live"]
        median = store_order_history_vals[f"{target_value}_median"]
        store_order_history_array = [
            store_history_live_vals[
                f"{target_value}_wsum_last_n_completed_orders_diff"
            ],
            store_history_live_vals[f"{target_value}_median_hour_diff"],
            store_history_live_vals[f"{target_value}_median_day_diff"],
            store_order_history_vals[f"{target_value}_median_week_diff"],
            0.0,
        ]
        first_non_null_value = next(
            item for item in store_order_history_array if not math.isnan(item)
        )
        most_granular_dict = {
            f"{target_value}_most_granular": median + first_non_null_value
        }
        logging.debug(f"{most_granular_dict}")
        return most_granular_dict

    def calculate_minutes_enroute_to_complete_most_granular_context(
        self, redis_data
    ):
        # Both of these are double dicts because the update call that loops through needs them to be their own dicts
        # Alternatively, that call could change to unpact dicts of dicts and otherwise add keys to the final set
        minutes_enroute_to_complete_most_granular = redis_data[
            "minutes_enroute_to_complete_most_granular"
        ]["minutes_enroute_to_complete_most_granular"]
        enroute_to_complete_zipwide_waterfall = redis_data[
            "enroute_to_complete_zipwide_waterfall"
        ]["enroute_to_complete_zipwide_waterfall"]
        minutes_enroute_to_complete_most_granular_context = (
            minutes_enroute_to_complete_most_granular
        )
        if enroute_to_complete_zipwide_waterfall is not None:
            minutes_enroute_to_complete_most_granular_context = (
                enroute_to_complete_zipwide_waterfall
                + minutes_enroute_to_complete_most_granular
            ) / 2.0
        val = {
            "minutes_enroute_to_complete_most_granular_context": minutes_enroute_to_complete_most_granular_context
        }
        logging.debug(val)
        return val

    def calculate_pct_capacity(self, redis_data):
        orders_per_driver_median = redis_data[
            "store_order_history"
        ].get("orders_per_driver_median")
        stores_active_drivers = redis_data["store_context"].get(
            "stores_active_drivers"
        )
        active_on_demand_orders = redis_data["store_context"][
            "stores_active_drivers"
        ]
        active_scheduled_orders = redis_data["store_context"][
            "stores_active_drivers"
        ]
        calculated_store_capacity = None
        if orders_per_driver_median is not None and stores_active_drivers is not None:
            calculated_store_capacity = orders_per_driver_median * stores_active_drivers
        if calculated_store_capacity is not None and calculated_store_capacity > 0.0:
            capacity_ratio = (
                active_on_demand_orders + active_scheduled_orders
            ) / calculated_store_capacity
            if capacity_ratio > 1.0:
                pct_capacity = 1.0
            else:
                pct_capacity = capacity_ratio
        elif active_on_demand_orders + active_scheduled_orders > 0.0:
            pct_capacity = 1.0
        else:
            pct_capacity = 0.0
        val = {"pct_capacity": pct_capacity}
        logging.debug(val)
        return val

    # The main 3 workers
    # First one gets from redis
    # Second one fills in missing values
    # Third one converts it to a dataframe and forms a request for every driver
    def get_redis_data(self, request_dict):
        store_metadata = self.get_store_metadata(request_dict)
        if len(store_metadata) == 0:
            logging.error(f"Missing store_metadata for {request_dict}")
            raise KeyError(f"Missing store_metadata for {request_dict}")
        hod, dow = self.get_local_hod_dow(store_metadata["time_zone"])
        request_dict["hod"] = hod
        request_dict["dow"] = dow
        store_order_history = self.get_store_order_history(request_dict)
        location_time_deltas = self.get_location_time_deltas(request_dict)
        store_context = self.get_store_context(request_dict)
        store_history_live = self.get_store_history_live(request_dict)
        available_driver_ids_list = store_context.get("available_driver_ids")
        driver_history = []
        # If there are drivers available, add in their history
        if available_driver_ids_list is not None:
            # available_driver_ids_list = json.loads(available_driver_ids_str)
            for available_driver_id in available_driver_ids_list:
                request_dict["driver_id"] = available_driver_id
                driver_history_value = self.get_driver_history(request_dict)
                if len(driver_history_value) < 1:
                    logging.debug(
                        f"No Driver History for driver_id:{available_driver_id}"
                    )
                else:
                    driver_history.append(driver_history_value)
        redis_data = {
            "store_metadata": store_metadata,
            "store_order_history": store_order_history,
            "location_time_deltas": location_time_deltas,
            "store_context": store_context,
            "store_history_live": store_history_live,
            "driver_history": driver_history,
        }
        return redis_data

    def process_redis_data(self, eta_request_dict, redis_data):
        # Fill in missing values
        if len(redis_data["driver_history"]) < 1:
            redis_data = self.fill_in_driver_history_nulls(redis_data)
        redis_data = self.fill_in_store_history_nulls(redis_data)
        redis_data = self.fill_in_store_context_nulls(redis_data)
        # Calculated values derived from others...
        redis_data["minutes_enroute_to_complete_waterfall"] = self.calculate_waterfall(
            "minutes_enroute_to_complete", redis_data
        )
        redis_data["minutes_placed_to_accepted_waterfall"] = self.calculate_waterfall(
            "minutes_placed_to_accepted", redis_data
        )
        redis_data["delivery_minutes_waterfall"] = self.calculate_waterfall(
            "delivery_minutes", redis_data
        )
        redis_data[
            "enroute_to_complete_zipwide_waterfall"
        ] = self.calculate_location_waterfall(
            eta_request_dict,
            redis_data["store_metadata"],
            redis_data["location_time_deltas"],
        )
        redis_data[
            "minutes_enroute_to_complete_most_granular"
        ] = self.calculate_most_granular("minutes_enroute_to_complete", redis_data)
        redis_data[
            "minutes_placed_to_accepted_most_granular"
        ] = self.calculate_most_granular("minutes_placed_to_accepted", redis_data)
        redis_data["delivery_minutes_most_granular"] = self.calculate_most_granular(
            "delivery_minutes", redis_data
        )
        redis_data[
            "minutes_enroute_to_complete_most_granular_context"
        ] = self.calculate_minutes_enroute_to_complete_most_granular_context(redis_data)
        redis_data["pct_capacity"] = self.calculate_pct_capacity(redis_data)
        return redis_data

    def coerce_request_dict(self, eta_request_dict):
        request_keys_to_type_dict = {
            "store_id": int,
            "delivery_zipcode": str
        }
        try:
            eta_request_dict_cleaned = self.transform_dict(eta_request_dict, request_keys_to_type_dict)
        except BadRequest as e:
            raise e
        return eta_request_dict_cleaned

    def get_data(self, eta_request_dict):
        logging.debug(f"eta_request_dict:{eta_request_dict}")
        eta_request_dict = self.coerce_request_dict(eta_request_dict)
        redis_data = self.get_redis_data(eta_request_dict)
        redis_data_processed = self.process_redis_data(eta_request_dict, redis_data)
        processed_request_dict = {}
        for keys, dictionaries in redis_data_processed.items():
            if keys != "driver_history":
                processed_request_dict.update(dictionaries)
        delivery_request_arr = []
        # TODO: Turn all of the requests into a single dataframe or do that outside of the data service instead of having each entry be a dataframe
        # this will make things faster
        for driver_request in redis_data["driver_history"]:
            # This will overwrite keys each time
            processed_request_dict.update(driver_request)
            delivery_request_arr.append(pd.DataFrame([processed_request_dict]))
        logging.info(f"Got redis data for request {eta_request_dict}")
        return delivery_request_arr

    def get_eta_fallback(self, eta_request_dict):
        try:
            namespace = "eta_fallback"
            namespace_id = eta_request_dict["store_id"]
            val = self._redis_client.hgetall(f"{namespace}:{namespace_id}")
            fallback_dict = {
                k: float(v) if k in ("eta_fallback_lower", "eta_fallback_upper") else v
                for k, v in val.items()
            }
            model_prediction = {"lower": fallback_dict["eta_fallback_lower"], "upper": fallback_dict["eta_fallback_upper"], "response_code": 202,
                                "error_message": "This store_id delivery_zipcode combo is not present in random forest model. Returning the fallback response"}
        except (TypeError, KeyError) as e:
            logging.error(
                f"store_id:{eta_request_dict['store_id']} not in fallback model"
            )
            model_prediction = {"response_code": 500,
                                "error_message": "This store_id  is not present in either the Random Forest Model or the Fallback model. This is expected on the first day for new stores, but shouldn't persist long after that."}
        finally:
            return model_prediction


# For testing
if __name__ == "__main__":
    ds = DataService()
    ds.set_redis_client(host="localhost")
    request = {"store_id": 5585, "delivery_zipcode": "64112", "hod": 12, "dow": "Wed"}
    # print(ds.get_store_history_live(request))
    # print(ds.get_from_redis(f"store_history_live:3041"))
    # print(ds._redis_client.hgetall(f"store_history_live:3041"))
    # ds.get_data({'store_id':-1, 'delivery_zipcode': '64112'})
    # ds.get_eta_fallback({'store_id':-1})
    print(ds.get_data(request, hod=request["hod"], dow=request["dow"]))


# List of broader refactors:
# TODO: Spark-redis connector has a shitty behavior where a key with a missing value is not pushed, Not crazy about handling in multiple places
# TODO: Migrate missing value processing to the get calls instead of process?
# TODO: For 3pd
# def get_courier_history(self):
#     # store_id
#     # courier_provider_name
#     # provider_minutes_placed_to_accepted_diff,
#     # provider_minutes_accepted_to_enroute_diff,
#     # provider_enroute_to_complete_diff,
#     # provider_delivery_minutes_diff,
#     return None
