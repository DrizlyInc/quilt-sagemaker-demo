import redis
import json
import datetime
import pytz
import pandas as pd
import math
#TODO: Add logging for debugging purposes
import logging
# logger = logging.getlogger()

class DataService:
    def __init__(self):
        self._redis_client = None

    def set_redis_client(self, host='redis', port=6379, db=0):
        self._redis_client = redis.StrictRedis(host=host, port=port, db=db, charset="utf-8", decode_responses=True)

    def get_from_redis(self, key):
        val = self._redis_client.get(key)
        logging.debug(f"Got key:{key} Value:{val}")
        return json.loads(val) if val is not None else None

    # "store_id",
    # "active_on_demand_orders",
    # "active_scheduled_orders",
    # "total_orders_preceding",
    # "stores_active_drivers",
    # "drivers_still_out_ct",
    # "drivers_returning_ct",
    # "is_a_driver_at_store",
    # "available_driver_ids",
    # "orders_per_driver_median",
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
    # zip
    # state
    # timezone
    # TODO: delivery_minutes_lower
    # TODO: delivery_minutes_upper
    def get_store_metadata(self, request_dict):
        namespace = "store_metadata"
        namespace_id = request_dict["store_id"]
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # "store_id",
    # "dow",
    # "hod",
    # "marking_enroute_poorly",
    # "orders_per_driver_median",
    # "minutes_placed_to_accepted_median",
    # "minutes_accepted_to_enroute_median",
    # "minutes_enroute_to_complete_median",
    # "delivery_minutes_median",
    # "minutes_placed_to_accepted_median_week_diff",
    # "minutes_accepted_to_enroute_median_week_diff",
    # "minutes_enroute_to_complete_median_week_diff",
    # "delivery_minutes_median_week_diff",
    # "enroute_to_complete_zipwide_extra_median_delta",
    # "enroute_to_complete_zipwide_intra_median_delta",
    # "enroute_to_complete_zipwide_dow_extra_median_delta",
    # "enroute_to_complete_zipwide_dow_intra_median_delta",
    # "enroute_to_complete_zipwide_hod_extra_median_delta",
    # "enroute_to_complete_zipwide_hod_intra_median_delta",
    # "enroute_to_complete_zipwide_dow_hod_extra_median_delta",
    # "enroute_to_complete_zipwide_dow_hod_intra_median_delta",
    # "enroute_to_complete_zipwide_waterfall",
    # "minutes_enroute_to_complete_stddev_hour",
    # "minutes_enroute_to_complete_stddev_day",
    # "minutes_enroute_to_complete_stddev_week",
    # "minutes_placed_to_accepted_stddev_hour",
    # "minutes_placed_to_accepted_stddev_day",
    # "minutes_placed_to_accepted_stddev_week",
    # "delivery_minutes_stddev_hour",
    # "delivery_minutes_stddev_day",
    # "delivery_minutes_stddev_week",
    # If this fails. The service cannot render a request
    # TODO: Think about filling in all zeroes in the event that values are missing, as well as a default value for a store_id for median
    # TODO: This value can be called by the adapter in the event that the predict path fails
    def get_store_order_history(self, request_dict):
        namespace = "store_order_history"
        namespace_id = f"{request_dict['store_id']},{request_dict['dow']},{request_dict['hod']}"
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    # store_id
    # driver_id
    # driver_minutes_placed_to_accepted_diff,
    # driver_minutes_accepted_to_enroute_diff,
    # driver_enroute_to_complete_diff,
    # driver_delivery_minutes_diff,
    #TODO: Handle missing values
    def get_driver_history(self, request_dict):
        namespace = "driver_history"
        namespace_id = f"{request_dict['store_id']},{request_dict['driver_id']}"
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    #TODO: For 3pd
    # def get_courier_history(self):
    #     # store_id
    #     # courier_provider_name
    #     # provider_minutes_placed_to_accepted_diff,
    #     # provider_minutes_accepted_to_enroute_diff,
    #     # provider_enroute_to_complete_diff,
    #     # provider_delivery_minutes_diff,
    #     return None




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
    def get_store_history_live(self, request_dict):
        namespace = "store_history_live"
        namespace_id = request_dict["store_id"]
        return self.get_from_redis(f"{namespace}:{namespace_id}")

    #TODO: These values below are all known at store_history_live compute time. It might be better to offload this logic
    #TODO: To the live portion of the data service to reduce amount that needs to live in this.
    def get_local_hod_dow(self, timezone):
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        timezone = pytz.timezone(timezone)
        local_time = timezone.normalize(now_utc.astimezone(timezone))
        hod = local_time.hour
        #dow as a string rather than an int
        dow = local_time.strftime('%a')
        logging.debug(f"Current dow:{dow} hod:{hod}")
        return hod, dow

    def fill_in_store_context_nulls(self, delivery_request_dict_with_data):
        if delivery_request_dict_with_data["store_context"] is None:
            missing_values_filled = {
    "active_on_demand_orders" : 0.0,
    "active_scheduled_orders" : 0.0,
    "total_orders_preceding" : 0.0,
    "stores_active_drivers" : 0.0,
    "drivers_still_out_ct" : 0.0,
    "drivers_returning_ct" : 0.0,
    "is_a_driver_at_store" : False,
    "available_driver_ids" : [],
    "orders_per_driver_median": 0.0}
        else:
            missing_values_filled = {}
            for k, v in delivery_request_dict_with_data["store_context"].items():
                if v is None or (k != "available_driver_ids" and math.isnan(v)):
                    if k == "is_a_driver_at_store":
                        val = False
                    else:
                        val = 0.0
                else:
                    val = v
                missing_values_filled[k] = val
            # Too many nested calls in the comprehension
            # missing_values_filled = {k: (v if not (math.isnan(v) or v is None) else False if k=="is_a_driver_at_store" else 0.0) for k, v in
            #                          delivery_request_dict_with_data["store_context"].items()}
        delivery_request_dict_with_data["store_context"] = missing_values_filled
        return delivery_request_dict_with_data

    def fill_in_driver_history_nulls(self, delivery_request_dict_with_data):
        logging.debug("No Driver History Present")
        no_driver_history_dict = {
            "driver_minutes_placed_to_accepted_diff": 0.0,
            "driver_minutes_accepted_to_enroute_diff": 0.0,
            "driver_enroute_to_complete_diff": 0.0,
            "driver_delivery_minutes_diff": 0.0,
        }
        delivery_request_dict_with_data["driver_history"] = [no_driver_history_dict]
        return delivery_request_dict_with_data

    def fill_in_store_history_nulls(self, delivery_request_dict_with_data):
        if delivery_request_dict_with_data["store_history_live"] is None:
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
            "delivery_minutes_wsum_last_n_completed_orders_diff": 0.0
            }
        else:
            missing_values_filled = {k: (v if not math.isnan(v) else 0.0) for k, v in delivery_request_dict_with_data["store_history_live"].items()}
        delivery_request_dict_with_data["store_history_live"] = missing_values_filled
        return delivery_request_dict_with_data

    def calculate_waterfall(self, target_value, delivery_request_dict_with_data):
        store_order_history_dict = delivery_request_dict_with_data["store_order_history"]
        store_history_live = delivery_request_dict_with_data["store_history_live"]
        median = store_order_history_dict[f"{target_value}_median"]
        stddev_hour = store_order_history_dict[f"{target_value}_stddev_hour"]
        stddev_day = store_order_history_dict[f"{target_value}_stddev_day"]
        stddev_week = store_order_history_dict[f"{target_value}_stddev_week"]
        wsum_last_n_completed_orders_diff = store_history_live[f"{target_value}_wsum_last_n_completed_orders_diff"]
        median_hour_diff = store_history_live[f"{target_value}_median_hour_diff"]
        median_day_diff = store_history_live[f"{target_value}_median_day_diff"]
        median_week_diff = store_order_history_dict[f"{target_value}_median_week_diff"]
        adding_value = median_week_diff
        if median_day_diff - 1.28*stddev_day <= wsum_last_n_completed_orders_diff <= median_day_diff + 1.28*stddev_day and median_hour_diff - 1.28*stddev_hour <= wsum_last_n_completed_orders_diff <= median_hour_diff + 1.28*stddev_hour:
            adding_value = wsum_last_n_completed_orders_diff
        elif median_hour_diff - 1.28*stddev_day <= median_hour_diff <=median_hour_diff + 1.28*stddev_day:
            adding_value = median_hour_diff
        elif median_week_diff - 1.28*stddev_week <= median_day_diff  <= median_week_diff + 1.28*stddev_week:
            adding_value = median_day_diff
        waterfall = median + adding_value
        logging.debug(f"Waterfall for:{target_value} = {waterfall}")
        return {f"{target_value}_waterfall": waterfall}

    def calculate_most_granular(self, target_value, delivery_request_dict_with_data):
        store_order_history_vals = delivery_request_dict_with_data["store_order_history"]
        store_history_live_vals = delivery_request_dict_with_data["store_history_live"]
        median = store_order_history_vals[f"{target_value}_median"]
        store_order_history_array = [store_history_live_vals[f"{target_value}_wsum_last_n_completed_orders_diff"],
        store_history_live_vals[f"{target_value}_median_hour_diff"],
        store_history_live_vals[f"{target_value}_median_day_diff"],
        store_order_history_vals[f"{target_value}_median_week_diff"],
         0.0]
        first_non_null_value = next(item for item in store_order_history_array if not math.isnan(item))
        most_granular_dict = {f"{target_value}_most_granular": median + first_non_null_value}
        logging.debug(f"{most_granular_dict}")
        return most_granular_dict

    def calculate_minutes_enroute_to_complete_most_granular_context(self, delivery_request_dict_with_data):
        minutes_enroute_to_complete_most_granular = delivery_request_dict_with_data["minutes_enroute_to_complete_most_granular"]["minutes_enroute_to_complete_most_granular"]
        enroute_to_complete_zipwide_waterfall = delivery_request_dict_with_data["store_order_history"]["enroute_to_complete_zipwide_waterfall"]
        minutes_enroute_to_complete_most_granular_context = minutes_enroute_to_complete_most_granular
        if enroute_to_complete_zipwide_waterfall is not None:
            minutes_enroute_to_complete_most_granular_context = (enroute_to_complete_zipwide_waterfall + minutes_enroute_to_complete_most_granular)/2.0
        val = {"minutes_enroute_to_complete_most_granular_context":minutes_enroute_to_complete_most_granular_context}
        logging.debug(val)
        return val

    def calculate_pct_capacity(self, delivery_request_dict_with_data):
        orders_per_driver_median = delivery_request_dict_with_data["store_context"]["orders_per_driver_median"]
        stores_active_drivers = delivery_request_dict_with_data["store_context"]["stores_active_drivers"]
        active_on_demand_orders = delivery_request_dict_with_data["store_context"]["stores_active_drivers"]
        active_scheduled_orders = delivery_request_dict_with_data["store_context"]["stores_active_drivers"]
        calculated_store_capacity = None
        if orders_per_driver_median is not None and stores_active_drivers is not None:
            calculated_store_capacity = orders_per_driver_median*stores_active_drivers
        if calculated_store_capacity > 0.0:
            capacity_ratio = (active_on_demand_orders + active_scheduled_orders) / calculated_store_capacity
            if capacity_ratio > 1.0:
                pct_capacity = 1.0
            else:
                pct_capacity = capacity_ratio
        elif active_on_demand_orders + active_scheduled_orders > 0.0:
            pct_capacity = 1.0
        else:
            pct_capacity = 0.0
        val = {"pct_capacity" : pct_capacity}
        logging.debug(val)
        return val

    #The main 3 workers
    #First one gets from redis
    #Second one fills in missing values
    #Third one converts it to a dataframe and forms a request for every driver


    def get_redis_data(self, request_dict, hod, dow):
        store_metadata = self.get_store_metadata(request_dict)
        if hod is None and dow is None:
            hod, dow = self.get_local_hod_dow(store_metadata['time_zone'])
        request_dict["hod"] = hod
        request_dict["dow"] = dow
        store_order_history = self.get_store_order_history(request_dict)
        store_context = self.get_store_context(request_dict)
        store_history_live = self.get_store_history_live(request_dict)
        available_driver_ids_str = store_context["available_driver_ids"]
        driver_history = []
        # If there are drivers available, add in their history
        if available_driver_ids_str is not None:
            available_driver_ids_list = json.loads(available_driver_ids_str)
            for available_driver_id in available_driver_ids_list:
                request_dict["driver_id"] = available_driver_id
                driver_history_value = self.get_driver_history(request_dict)
                if driver_history_value is None:
                    logging.debug(f"No Driver History for driver_id:{available_driver_id}")
                else:
                    driver_history.append(driver_history_value)
        delivery_requests_dict = {"store_metadata": store_metadata,
                                       "store_order_history": store_order_history,
                                       "store_context": store_context,
                                       "store_history_live": store_history_live,
                                       "driver_history": driver_history}
        return delivery_requests_dict


    def process_redis_data(self, redis_data):
        # Fill in missing values
        if len(redis_data["driver_history"]) < 1:
            redis_data = self.fill_in_driver_history_nulls(redis_data)
        redis_data = self.fill_in_store_history_nulls(redis_data)
        redis_data = self.fill_in_store_context_nulls(redis_data)
        # Calculated values derived from others...
        redis_data["minutes_enroute_to_complete_waterfall"] = self.calculate_waterfall("minutes_enroute_to_complete", redis_data)
        redis_data["minutes_placed_to_accepted_waterfall"] = self.calculate_waterfall("minutes_placed_to_accepted", redis_data)
        redis_data["delivery_minutes_waterfall"] = self.calculate_waterfall("delivery_minutes",redis_data)
        redis_data["minutes_enroute_to_complete_most_granular"] = self.calculate_most_granular("minutes_enroute_to_complete", redis_data)
        redis_data["minutes_placed_to_accepted_most_granular"] = self.calculate_most_granular("minutes_placed_to_accepted", redis_data)
        redis_data["delivery_minutes_most_granular"] = self.calculate_most_granular("delivery_minutes", redis_data)
        redis_data["minutes_enroute_to_complete_most_granular_context"] = self.calculate_minutes_enroute_to_complete_most_granular_context(redis_data)
        redis_data["pct_capacity"] = self.calculate_pct_capacity(redis_data)
        return redis_data

    def get_data(self, eta_request_dict, hod=None, dow=None):
        logging.debug(f"eta_request_dict:{eta_request_dict}")
        redis_data = self.get_redis_data(eta_request_dict, hod, dow)
        redis_data_processed = self.process_redis_data(redis_data)
        processed_request_dict = {}
        for keys, dictionaries in redis_data_processed.items():
            if keys != "driver_history":
                processed_request_dict.update(dictionaries)
        delivery_request_arr = []
        #TODO: Turn the whole thing into a dataframe or do that outside of the data service instead of having each entry be a dataframe
        for driver_request in redis_data["driver_history"]:
            #This will overwrite keys each time
            processed_request_dict.update(driver_request)
            delivery_request_arr.append(pd.DataFrame([processed_request_dict]))
        return delivery_request_arr

    def get_eta_fallback(self, eta_request_dict):
        try:
            namespace = "eta_fallback"
            namespace_id = eta_request_dict["store_id"]
            val = self._redis_client.get(f"{namespace}:{namespace_id}")
            fallback_dict = json.loads(val)
            return fallback_dict['eta_fallback_lower'], fallback_dict['eta_fallback_upper']
        except TypeError as e:
            logging.warning(f"store_id:{eta_request_dict['store_id']} not in fallback model")
            return []

# For testing
if __name__ == '__main__':
    ds = DataService()
    ds.set_redis_client(host='localhost')
    request = {'store_id': 270, 'delivery_zipcode': '64112', 'hod': 12, 'dow': 'Wed'}
    ds.get_eta_fallback({'store_id':-1})
    print(ds.get_data(request, hod=request['hod'], dow=request['dow']))