import redis
import json
import sys

sys.path.append("..")

from app.eta.util import SnowflakeConnector

redis_client = redis.StrictRedis(
    host="redis", port=6379, db=0, charset="utf-8", decode_responses=True
)


def get_training_rows(target_db):
    sql = """
    SELECT *
    FROM {target_db}.datascience.etas_training_no_3pd;
    """
    return SnowflakeConnector.get_dataframe_from_sql(sql.format(target_db=target_db))


def write_to_redis(redis_connection, df, columns, namespace, namespace_id_columns):
    # Take only one value per namespace key
    df_unique = (
        df[columns]
        .groupby(namespace_id_columns, group_keys=False)
        .apply(lambda df: df.sample(1))
    )
    df_dict = df_unique[columns].to_dict("records")
    for row in df_dict:
        namespace_ids = []
        for namespace_id_column in namespace_id_columns:
            namespace_ids.append(str(row[namespace_id_column]))
        row.update({namespace: ",".join(namespace_ids)})
    # Stringify the above result (this is basically an index)
    # Pop call is just to remove the namespace key
    df_dict_with_namespace = {f"{namespace}:{row[namespace]}": row for row in df_dict}
    # df_dict_with_namespace_post_pop = {k : (v_dict, v_dict.pop(namespace))[0] for k,v_dict in df_dict_with_namespace.items()}
    # This writes the key as a value
    print("Writing " + namespace)
    with redis_connection.pipeline() as pipe:
        for namespace_id, values in df_dict_with_namespace.items():
            # Remove duplicate data in the values
            values.pop(namespace)
            for namespace_column in namespace_id_columns:
                values.pop(namespace_column)
            pipe.set(namespace_id, json.dumps(values))
        pipe.execute()
    print("Written: " + str(len(df_dict_with_namespace)))


def set_store_context(redis_connection, df):
    # TODO: Wire this up to configuration along with train and predict
    namespace = "store_context"
    namespace_id_columns = ["store_id"]
    store_context_columns = [
        "store_id",
        "active_on_demand_orders",
        "active_scheduled_orders",
        "total_orders_preceding",
        "stores_active_drivers",
        "drivers_still_out_ct",
        "drivers_returning_ct",
        "is_a_driver_at_store",
        "available_driver_ids",
        "orders_per_driver_median",
    ]
    write_to_redis(
        redis_connection, df, store_context_columns, namespace, namespace_id_columns
    )


#
# #TODO: static data
# #write daily
#       TODO: delivery_minutes_lower
#     # TODO: delivery_minutes_upper
def set_store_metadata(redis_connection, df):
    namespace = "store_metadata"
    namespace_id_columns = ["store_id"]
    store_metadata_columns = ["store_id", "store_zipcode", "time_zone"]
    write_to_redis(
        redis_connection, df, store_metadata_columns, namespace, namespace_id_columns
    )
    # zip
    # state


#
# #Write daily
## TODO: Need to create the waterfall value in the data service
def set_store_history(redis_connection, df):
    namespace = "store_order_history"
    namespace_id_columns = ["store_id", "dow", "hod"]
    store_history_columns = [
        "store_id",
        "dow",
        "hod",
        "marking_enroute_poorly",
        "orders_per_driver_median",
        "minutes_placed_to_accepted_median",
        "minutes_accepted_to_enroute_median",
        "minutes_enroute_to_complete_median",
        "delivery_minutes_median",
        "minutes_placed_to_accepted_median_week_diff",
        "minutes_accepted_to_enroute_median_week_diff",
        "minutes_enroute_to_complete_median_week_diff",
        "delivery_minutes_median_week_diff",
        "enroute_to_complete_zipwide_extra_median_delta",
        "enroute_to_complete_zipwide_intra_median_delta",
        "enroute_to_complete_zipwide_dow_extra_median_delta",
        "enroute_to_complete_zipwide_dow_intra_median_delta",
        "enroute_to_complete_zipwide_hod_extra_median_delta",
        "enroute_to_complete_zipwide_hod_intra_median_delta",
        "enroute_to_complete_zipwide_dow_hod_extra_median_delta",
        "enroute_to_complete_zipwide_dow_hod_intra_median_delta",
        "enroute_to_complete_zipwide_waterfall",
        "minutes_enroute_to_complete_stddev_hour",
        "minutes_enroute_to_complete_stddev_day",
        "minutes_enroute_to_complete_stddev_week",
        "minutes_placed_to_accepted_stddev_hour",
        "minutes_placed_to_accepted_stddev_day",
        "minutes_placed_to_accepted_stddev_week",
        "delivery_minutes_stddev_hour",
        "delivery_minutes_stddev_day",
        "delivery_minutes_stddev_week",
    ]
    write_to_redis(
        redis_connection, df, store_history_columns, namespace, namespace_id_columns
    )


#
# # Write daily
def set_driver_history(redis_connection, df):
    namespace = "driver_history"
    namespace_id_columns = ["store_id", "driver_id"]
    driver_history_columns = [
        "store_id",
        "driver_id",
        "driver_minutes_placed_to_accepted_diff",
        "driver_minutes_accepted_to_enroute_diff",
        "driver_enroute_to_complete_diff",
        "driver_delivery_minutes_diff",
    ]
    write_to_redis(
        redis_connection, df, driver_history_columns, namespace, namespace_id_columns
    )


# TODO: 3pd values
# # Write daily
# def set_provider_history():
#     hash: provider_history
#     keys: store_id
#     provider_minutes_placed_to_accepted_diff,
#     provider_minutes_accepted_to_enroute_diff,
#     provider_enroute_to_complete_diff,
#     provider_delivery_minutes_diff,
#
# #Write every 3-5 minutes
def set_store_history_live(redis_connection, df):
    namespace = "store_history_live"
    namespace_id_columns = ["store_id"]
    store_history_columns = [
        "store_id",
        "minutes_placed_to_accepted_median_day_diff",
        "minutes_accepted_to_enroute_median_day_diff",
        "minutes_enroute_to_complete_median_day_diff",
        "delivery_minutes_median_day_diff",
        "minutes_placed_to_accepted_median_hour_diff",
        "minutes_accepted_to_enroute_median_hour_diff",
        "minutes_enroute_to_complete_median_hour_diff",
        "delivery_minutes_median_hour_diff",
        "minutes_placed_to_accepted_wsum_last_n_completed_orders_diff",
        "minutes_accepted_to_enroute_wsum_last_n_completed_orders_diff",
        "minutes_enroute_to_complete_wsum_last_n_completed_orders_diff",
        "delivery_minutes_wsum_last_n_completed_orders_diff",
    ]

    write_to_redis(
        redis_connection, df, store_history_columns, namespace, namespace_id_columns
    )

def set_eta_fallback(target_db, redis_connection):
    sql = """
        SELECT store_id, 
GREATEST(delivery_minutes_median - coalesce(1.28*stddev_delivery_minutes, delivery_minutes_median*0.1), 10.0) eta_fallback_lower, 
delivery_minutes_median + coalesce(1.28*stddev_delivery_minutes, delivery_minutes_median*0.2) eta_fallback_upper
FROM
(SELECT store_id, median(delivery_minutes) delivery_minutes_median, stddev(delivery_minutes) stddev_delivery_minutes 
 FROM PROD.REPORTING.STORE_ORDERS_COMPLETED_CORE 
 WHERE FULFILLMENT_TYPE = 'On Demand Delivery' 
 GROUP BY 1) as T;
        """
    eta_fallback_df = SnowflakeConnector.get_dataframe_from_sql(sql.format(target_db=target_db))
    namespace = "eta_fallback"
    namespace_id_columns = ["store_id"]
    eta_fallback_columns = [
        "store_id",
        "eta_fallback_lower",
        "eta_fallback_upper"
    ]
    write_to_redis(
        redis_connection, eta_fallback_df, eta_fallback_columns, namespace, namespace_id_columns
    )

if __name__ == "__main__":
    redis_connection = redis.StrictRedis(host='redis', port=6379, db=0, charset="utf-8", decode_responses=True)
    # for local dev:
    # redis_connection = redis.StrictRedis(
    #     host="localhost", port=6379, db=0, charset="utf-8", decode_responses=True
    # )
    redis_connection.flushall()
    if redis_connection.ping():
        print("Connected to redis")
    df = get_training_rows("stage_js")
    set_eta_fallback("stage_js", redis_connection)
    print("Got Training Data")
    set_store_metadata(redis_connection, df)
    set_store_context(redis_connection, df)
    set_driver_history(redis_connection, df)
    set_store_history(redis_connection, df)
    set_store_history_live(redis_connection, df)

