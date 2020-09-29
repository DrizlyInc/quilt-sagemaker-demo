from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import boto3
import json
import logging
import datetime

# sc = SparkContext("local", "Simple App")
# Spark version 2.4.3
# Using Scala version 2.11.12

spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .config("spark.debug.maxToStringFields", 10000)
    .config("spark.redis.host", "eta-redis.dgvvsn.0001.use1.cache.amazonaws.com")
    .getOrCreate()
)
# confirmed to work:
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(
    spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
)
# TODO: Sort out for EMR jobs
# --packages net.snowflake:snowflake-jdbc:3.8.0,net.snowflake:spark-snowflake_2.12:2.5.1-spark_2.4,mysql:mysql-connector-java:5.1.39,
# SPARK 2.4= venv/bin/pyspark --packages net.snowflake:snowflake-jdbc:3.12.8,net.snowflake:spark-snowflake_2.11:2.8.0-spark_2.4,mysql:mysql-connector-java:5.1.39,com.redislabs:spark-redis:2.4.0
# venv/bin/pyspark --packages net.snowflake:snowflake-jdbc:3.8.0,net.snowflake:spark-snowflake_2.12:2.5.1-spark_2.4
# TODO: Change redis host to config
spark_conf = (
    SparkConf()
    .setMaster("local")
    .setAppName("etav2-data-service")
    .set("spark.redis.host", "eta-redis.dgvvsn.0001.use1.cache.amazonaws.com")
)

snowflake_store_order_history_query = """
SELECT store_history.*,
minutes_placed_to_accepted_median_week - minutes_placed_to_accepted_median minutes_placed_to_accepted_median_week_diff,
minutes_accepted_to_enroute_median_week - minutes_accepted_to_enroute_median minutes_accepted_to_enroute_median_week_diff,
minutes_enroute_to_complete_median_week - minutes_enroute_to_complete_median minutes_enroute_to_complete_median_week_diff,
delivery_minutes_median_week - delivery_minutes_median delivery_minutes_median_week_diff,
etas_store_calculated_efficiency.orders_per_driver_median
FROM
(SELECT
store_id,
median(minutes_placed_to_accepted)  minutes_placed_to_accepted_median,
median(minutes_accepted_to_enroute) minutes_accepted_to_enroute_median,
median(minutes_enroute_to_complete)  minutes_enroute_to_complete_median,
median(delivery_minutes) delivery_minutes_median,
median(case when datediff(d,sent_to_store_at, current_date) between 1 and 7  then minutes_placed_to_accepted end) minutes_placed_to_accepted_median_week,
median(case when datediff(d,sent_to_store_at, current_date) between 1 and 7  then minutes_accepted_to_enroute end) minutes_accepted_to_enroute_median_week,
median(case when datediff(d,sent_to_store_at, current_date) between 1 and 7  then minutes_enroute_to_complete end) minutes_enroute_to_complete_median_week,
median(case when datediff(d,sent_to_store_at, current_date) between 1 and 7 then delivery_minutes end) delivery_minutes_median_week,
stddev(minutes_placed_to_accepted)  minutes_placed_to_accepted_stddev,
stddev(minutes_accepted_to_enroute) minutes_accepted_to_enroute_stddev,
stddev(minutes_enroute_to_complete)  minutes_enroute_to_complete_stddev,
stddev(delivery_minutes) delivery_minutes_stddev,
stddev(case when datediff(d,sent_to_store_at, current_date) between 1 and 7  then minutes_placed_to_accepted end) minutes_placed_to_accepted_stddev_week,
stddev(case when datediff(d,sent_to_store_at, current_date) between 1 and 7  then minutes_accepted_to_enroute end) minutes_accepted_to_enroute_stddev_week,
stddev(case when datediff(d,sent_to_store_at, current_date) between 1 and 7  then minutes_enroute_to_complete end) minutes_enroute_to_complete_stddev_week,
stddev(case when datediff(d,sent_to_store_at, current_date) between 1 and 7 then delivery_minutes end) delivery_minutes_stddev_week,
count(distinct store_order_id) order_ct,
sum(
case when minutes_enroute_to_complete < 5.0 and minutes_accepted_to_enroute / delivery_minutes > 0.5
then 1 else 0 end) / count(*)  > 0.30 marking_enroute_poorly,
max(current_date) mx_date
FROM {snowflake_schema}.datascience.etas_filtered_core_store_orders AS U
WHERE datediff(d, order_date, current_date)  < 30 --TODO: Lookback
GROUP BY 1) as store_history
LEFT OUTER JOIN {snowflake_schema}.datascience.etas_store_calculated_efficiency
ON store_history.store_id = etas_store_calculated_efficiency.store_id
"""

snowflake_store_metadata_query = f"""SELECT DISTINCT id as store_id, zip as store_zipcode, time_zone, current_date mx_date
FROM raw.mysql_drizly.stores
"""

# TODO: The partition over delivery_state isn't necessary.
snowflake_location_time_deltas_query = """
select * 
FROM (SELECT *, rank() over (partition by delivery_zipcode, dow, hod order by training_data_date desc) rk, current_date as mx_date
FROM {snowflake_schema}.datascience.ETAS_LOCATION_TIME_DELTAS 
WHERE DOW in (dayname(current_date), dayname(current_date + 1))) 
WHERE rk = 1 ;
"""

snowflake_eta_fallback_query = """SELECT store_id, 
GREATEST(delivery_minutes_median - coalesce(1.28*stddev_delivery_minutes, delivery_minutes_median*0.1), 10.0) eta_fallback_lower, 
delivery_minutes_median + coalesce(1.28*stddev_delivery_minutes, delivery_minutes_median*0.2) eta_fallback_upper,
current_date as mx_date
FROM
(SELECT store_id, median(delivery_minutes) delivery_minutes_median, stddev(delivery_minutes) stddev_delivery_minutes 
FROM {snowflake_schema}.REPORTING.STORE_ORDERS_COMPLETED_CORE 
WHERE FULFILLMENT_TYPE = 'On Demand Delivery' 
GROUP BY 1) as T;"""

snowflake_driver_delivery_history_query = """
SELECT
store_id,
driver_id,
median(minutes_placed_to_accepted - minutes_placed_to_accepted_median)  driver_minutes_placed_to_accepted_diff,
median(minutes_accepted_to_enroute - minutes_accepted_to_enroute_median) driver_minutes_accepted_to_enroute_diff,
median(minutes_enroute_to_complete - minutes_enroute_to_complete_median)  driver_minutes_enroute_to_complete_diff,
median(delivery_minutes - delivery_minutes_median) driver_delivery_minutes_diff,
max(mx_date) mx_date
from
(select 
store_id,
driver_id,
minutes_placed_to_accepted,
minutes_accepted_to_enroute,
minutes_enroute_to_complete,
delivery_minutes,
median(minutes_placed_to_accepted) over (partition by store_id) minutes_placed_to_accepted_median,
median(minutes_accepted_to_enroute) over (partition by store_id) minutes_accepted_to_enroute_median,
median(minutes_enroute_to_complete) over (partition by store_id) minutes_enroute_to_complete_median,
median(delivery_minutes) over (partition by store_id) delivery_minutes_median,
current_date as mx_date
FROM {snowflake_schema}.datascience.etas_filtered_core_store_orders
WHERE datediff(d, order_date, current_date)  < 30 --TODO: Lookback
AND driver_id is not null
) as T
GROUP BY 1,2
"""

mysql_past_24_hours_of_store_events = """SELECT T.id store_order_event_id, T.store_id, T.order_id, U.id store_order_id, T.status, T.event_time, U.driver_id, U.delivery_type 
FROM (SELECT store_id, order_id, status, event_time, id 
FROM {db_schema}.store_order_events 
WHERE id > {most_recent_store_order_event_id} ORDER BY 1 , 2) AS T  
JOIN {db_schema}.store_orders AS U 
ON T.store_id = U.store_id AND T.order_id = U.order_id 
WHERE T.event_time > '{ts_24_hours_ago}' ORDER BY T.order_id"""

spark_store_orders_with_times = """SELECT
store_id,
driver_id,
store_order_id,
store_order_placed_at,
store_order_accepted_at,
store_order_enroute_at store_order_enroute_at,
store_order_outside_at store_order_outside_at,
store_order_completed_at store_order_completed_at,
most_recent_event,
delivery_type
FROM
(SELECT 
store_id,
driver_id,
store_order_id,
delivery_type,
min(case when status = 1 then event_time end) store_order_placed_at,
min(case when status = 2 then event_time end) store_order_accepted_at,
min(case when status = 3 then event_time end) store_order_enroute_at,
min(case when status = 4 then event_time end) store_order_outside_at,
min(case when status = 6 then event_time end) store_order_completed_at,
min(case when status = 7 then event_time end) store_order_voided_at,
max(event_time) most_recent_event
FROM mysql_past_24_hours_of_store_events
GROUP BY 1,2,3,4) as T
WHERE store_order_voided_at is null
AND store_order_placed_at is not null
ORDER BY store_id, store_order_placed_at"""

# This messy concat makes a serialized python array from a wrapped array returned by collect_set
spark_active_drivers = """select store_id,
concat('[', concat_ws(',', COLLECT_SET(
CASE WHEN unix_timestamp(most_recent_event_driver) > unix_timestamp() -{lookback_hours} * 60 * 60 THEN driver_id END
)), ']') available_driver_ids,
greatest(
COUNT(
DISTINCT CASE WHEN most_recent_event_driver > from_unixtime(
unix_timestamp() -{lookback_hours} * 60 * 60
) THEN driver_id END
), 
0
) stores_active_drivers, 
count(distinct case when completed_orders = driver_orders_ct AND unix_timestamp(store_order_completed_at_mx) + travel_time > unix_timestamp() THEN driver_id END) drivers_returning_ct,
count(distinct case when store_order_enroute_at_mx is not null and ((store_order_completed_at_mx is null and  unix_timestamp(store_order_enroute_at_mx) > unix_timestamp() - 600) or
store_order_enroute_at_mx < store_order_completed_at_mx) then driver_id end) drivers_still_out_ct
FROM
(SELECT STORE_ID,
DRIVER_ID,
count(*) driver_orders_ct,
sum(case when store_order_completed_at is not null then 1 else 0 end) completed_orders,
avg(unix_timestamp(store_order_completed_at) - unix_timestamp(store_order_enroute_at)) travel_time,
max(store_order_completed_at) store_order_completed_at_mx,
max(case when store_order_completed_at is null then store_order_enroute_at end) store_order_enroute_at_mx,
max(most_recent_event) most_recent_event_driver
FROM store_orders_with_times
WHERE most_recent_event > from_unixtime(unix_timestamp() - {lookback_hours} * 60 * 60) 
AND delivery_type in (5,6)
GROUP BY 1,2) as T
GROUP BY 1
"""

spark_active_orders = """ 
SELECT 
store_id, 
COUNT(
DISTINCT CASE WHEN unix_timestamp(store_order_placed_at) > unix_timestamp() -{lookback_hours} * 60 * 60 
AND store_order_completed_at IS NULL 
AND delivery_type in (5, 6) THEN store_order_id END
) total_orders_preceding, 
COUNT(
DISTINCT CASE WHEN unix_timestamp(store_order_placed_at) > unix_timestamp() -{lookback_hours} * 60 * 60 
AND store_order_completed_at IS NULL 
AND delivery_type = 5 THEN store_order_id END) active_on_demand_orders, 
COUNT(
DISTINCT CASE WHEN unix_timestamp(store_order_placed_at) > unix_timestamp() -{lookback_hours} * 60 * 60 
AND store_order_completed_at IS NULL 
AND delivery_type = 6 THEN store_order_id END 
) active_scheduled_orders
FROM 
store_orders_with_times
GROUP BY 
1
"""
# TODO: This might be the finished form of the push up to redis
spark_store_context = """
SELECT active_orders.store_id,
active_on_demand_orders,
active_scheduled_orders,
total_orders_preceding,
stores_active_drivers,
drivers_still_out_ct, 
drivers_returning_ct,
stores_active_drivers > (drivers_still_out_ct + drivers_returning_ct) is_a_driver_at_store,
available_driver_ids
FROM active_orders
JOIN active_drivers
ON active_orders.store_id = active_drivers.store_id
"""
# TODO: This might be the finished form of what needs to be pushed up to redis
spark_store_history_live = """
SELECT store_order_history_live_precompute.store_id,
minutes_placed_to_accepted_median_day - minutes_placed_to_accepted_median minutes_placed_to_accepted_median_day_diff,
minutes_accepted_to_enroute_median_day - minutes_accepted_to_enroute_median minutes_accepted_to_enroute_median_day_diff,
minutes_enroute_to_complete_median_day - minutes_enroute_to_complete_median minutes_enroute_to_complete_median_day_diff,
delivery_minutes_median_day - delivery_minutes_median delivery_minutes_median_day_diff,
minutes_placed_to_accepted_median_hour - minutes_placed_to_accepted_median minutes_placed_to_accepted_median_hour_diff,
minutes_accepted_to_enroute_median_hour - minutes_accepted_to_enroute_median minutes_accepted_to_enroute_median_hour_diff,
minutes_enroute_to_complete_median_hour - minutes_enroute_to_complete_median minutes_enroute_to_complete_median_hour_diff,
delivery_minutes_median_hour - delivery_minutes_median delivery_minutes_median_hour_diff,
minutes_placed_to_accepted_wsum_last_n_completed_orders - minutes_placed_to_accepted_median minutes_placed_to_accepted_wsum_last_n_completed_orders_diff,
minutes_accepted_to_enroute_wsum_last_n_completed_orders - minutes_accepted_to_enroute_median minutes_accepted_to_enroute_wsum_last_n_completed_orders_diff,
minutes_enroute_to_complete_wsum_last_n_completed_orders - minutes_enroute_to_complete_median minutes_enroute_to_complete_wsum_last_n_completed_orders_diff,
delivery_minutes_wsum_last_n_completed_orders - delivery_minutes_median delivery_minutes_wsum_last_n_completed_orders_diff,
minutes_enroute_to_complete_stddev_hour,
minutes_enroute_to_complete_stddev_day,
minutes_placed_to_accepted_stddev_hour,
minutes_placed_to_accepted_stddev_day,
delivery_minutes_stddev_hour,
delivery_minutes_stddev_day
FROM
(select store_id,
percentile_approx(minutes_placed_to_accepted, 0.5) minutes_placed_to_accepted_median_day,
percentile_approx(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then minutes_placed_to_accepted end, 0.5) minutes_placed_to_accepted_median_hour,
percentile_approx(case when rk < {last_n_orders_plus_one} then minutes_placed_to_accepted end, 0.5) minutes_placed_to_accepted_wsum_last_n_completed_orders,
stddev(minutes_placed_to_accepted) minutes_placed_to_accepted_stddev_day,
stddev(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then minutes_placed_to_accepted end) minutes_placed_to_accepted_stddev_hour,
percentile_approx(minutes_accepted_to_enroute, 0.5) minutes_accepted_to_enroute_median_day,
percentile_approx(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then minutes_accepted_to_enroute end, 0.5) minutes_accepted_to_enroute_median_hour,
percentile_approx(case when rk < {last_n_orders_plus_one} then minutes_accepted_to_enroute end, 0.5) minutes_accepted_to_enroute_wsum_last_n_completed_orders,
stddev(minutes_accepted_to_enroute) minutes_accepted_to_enroute_stddev_day,
stddev(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then minutes_accepted_to_enroute end) minutes_accepted_to_enroute_stddev_hour,
percentile_approx(minutes_enroute_to_complete, 0.5) minutes_enroute_to_complete_median_day,
percentile_approx(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then minutes_enroute_to_complete end, 0.5) minutes_enroute_to_complete_median_hour,
percentile_approx(case when rk < {last_n_orders_plus_one} then minutes_enroute_to_complete end, 0.5) minutes_enroute_to_complete_wsum_last_n_completed_orders,
stddev(minutes_enroute_to_complete) minutes_enroute_to_complete_stddev_day,
stddev(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then minutes_enroute_to_complete end) minutes_enroute_to_complete_stddev_hour,
percentile_approx(delivery_minutes, 0.5) delivery_minutes_median_day,
percentile_approx(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then delivery_minutes end, 0.5) delivery_minutes_median_hour,
percentile_approx(case when rk < {last_n_orders_plus_one} then delivery_minutes end, 0.5) delivery_minutes_wsum_last_n_completed_orders,
stddev(delivery_minutes) delivery_minutes_stddev_day,
stddev(case when store_order_placed_at > from_unixtime(unix_timestamp() - 3600) then delivery_minutes end) delivery_minutes_stddev_hour
FROM 
(select store_id,
store_order_placed_at,
rank() over (partition by store_id order by store_order_completed_at desc) rk,
(unix_timestamp(store_order_accepted_at) - unix_timestamp(store_order_placed_at))/60.0 minutes_placed_to_accepted,
(unix_timestamp(store_order_enroute_at) - unix_timestamp(store_order_accepted_at))/60.0 minutes_accepted_to_enroute,
(unix_timestamp(store_order_completed_at) - unix_timestamp(store_order_enroute_at))/60.0 minutes_enroute_to_complete,
(unix_timestamp(store_order_completed_at) - unix_timestamp(store_order_placed_at))/60.0 delivery_minutes
from store_orders_with_times
where delivery_type = 5) as T
GROUP BY 1) as store_order_history_live_precompute
JOIN store_order_history
ON store_order_history_live_precompute.store_id  = store_order_history.store_id
"""

# TODO: Define lookback_hours = 2, last_n_orders
def read_from_mysql(spark, query, mysql_creds):
    host = mysql_creds["host"]
    port = mysql_creds["port"]
    database = mysql_creds["dbname"]
    username = mysql_creds["username"]
    password = mysql_creds["password"]
    url = f"jdbc:mysql://{host}:{port}/{database}"
    driver = "com.mysql.jdbc.Driver"
    df = (
        spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", f"({query}) as dbTable")
        .option("user", username)
        .option("password", password)
        .load()
    )
    return df
    # url = f"jdbc:mysql://${host}:${port}/${database}?user=${username}&password=${password}&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"


# For long term data and/or for testing purposes
def read_from_snowflake(
    spark, query, snowflake_creds, warehouse="REPORT", schema="TEST", role="analyst"
):
    account = snowflake_creds["account"]
    user_name = snowflake_creds["username"]
    password = snowflake_creds["password"]
    database = snowflake_creds["database"]
    schema = schema
    warehouse = warehouse
    role = role
    # Set options below
    sfOptions = {
        "sfURL": f"{account}" + ".snowflakecomputing.com",
        "sfUser": f"{user_name}",
        "sfPassword": f"{password}",
        "sfDatabase": f"{database}",
        "sfSchema": f"{schema}",
        "sfWarehouse": f"{warehouse}",
    }
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    df = (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("query", query)
        .load()
    )
    return df


def load_snowflake_df(spark, path, df_name):
    try:
        df = spark.read.format("parquet").load(f"{path}/{df_name}")
        logging.warning(f"Loaded {df_name} from {path}/{df_name}")
        # We want to refresh following an expected dbt run (UTC noon)
        mx_date = df.agg({"mx_date": "max"}).collect()[0][0]
        if (
            datetime.datetime.combine(mx_date, datetime.time())
            + datetime.timedelta(days=1, hours=12)
            > datetime.datetime.utcnow()
        ):
            return df
        else:
            return None
    except Exception as e:
        logging.exception(e)
        logging.warning(f"No {df_name} data at:{path}")
        return None


def load_mysql_df(spark, parquet_path, df_name, mysql_creds):
    ts_24_hours_ago = (
        datetime.datetime.utcnow() - datetime.timedelta(hours=24)
    ).strftime("%Y-%m-%d %H:%M:%S")
    most_recent_store_order_event_id = None
    try:
        stored_df = (
            spark.read.format("parquet")
            .load(f"{parquet_path}/{df_name}")
            .where("event_time > current_timestamp() - INTERVAL 1 DAY")
        )
        # We want to refresh following an expected dbt run (UTC noon)
        most_recent_store_order_event_id = stored_df.agg(
            {"store_order_event_id": "max"}
        ).collect()[0][0]
        new_df = read_from_mysql(
            spark,
            mysql_past_24_hours_of_store_events.format(
                most_recent_store_order_event_id=most_recent_store_order_event_id,
                db_schema="drizly",
                ts_24_hours_ago=ts_24_hours_ago,
            ),
            mysql_creds,
        )
        return stored_df.unionAll(new_df)
    except Exception as e:
        logging.exception(e)
        logging.warning(f"No {df_name} data at parquet_path:{parquet_path}/{df_name}")
        logging.warning(
            """The above exception can happen any time the script is run for the first time on an instance. It isn't a critical error, but if it happens repeatedly it means something is wrong with final call in the main() function"""
        )
        # Get all events from the past 24 hours by setting most recent event id to 63323874 (Most recent event at the time this was written)
        if most_recent_store_order_event_id is not None:
            logging.warning(
                f"Most Recent Store Order Event Id: {most_recent_store_order_event_id}"
            )
        logging.warning(
            f"""MySQL Query:{mysql_past_24_hours_of_store_events.format(most_recent_store_order_event_id=63323874,
                                                                                   db_schema="drizly",
                                                                                   ts_24_hours_ago = ts_24_hours_ago)}"""
        )
        return read_from_mysql(
            spark,
            mysql_past_24_hours_of_store_events.format(
                most_recent_store_order_event_id=63323874,
                db_schema="drizly",
                ts_24_hours_ago=ts_24_hours_ago,
            ),
            mysql_creds,
        )


# TODO: When this is called it should also write the appropriate keys to redis
def get_snowflake_df(spark, snowflake_creds, parquet_path, query, df_name):
    df = load_snowflake_df(spark, parquet_path, df_name)
    stale_data = df is None
    if stale_data:
        logging.warning(f"Pulling fresh data for: {df_name}")
        df = read_from_snowflake(spark, query, snowflake_creds)
    return df, stale_data


# TODO: Define a ttl.
def push_df_to_redis(spark, registeredTempTable, namespace_id_columns):
    namespace_id_columns_joined = ",".join(namespace_id_columns)
    # TODO: If you rewrite this so it doesn't require array_join... programmatically generating nested concats with the correct columns
    # you might get faster peformance as pushdown fails solely due to this and that can affect what the optimizer does
    logging.info(f"Writing to redis with config:")
    configurations = spark.sparkContext.getConf().getAll()
    for configuration in configurations:
        logging.info(f"{configuration}")
    for conf in configurations:
        print(conf)
    df = spark.sql(
        f"SELECT *, concat_ws(',',array({namespace_id_columns_joined}) ) namespace_id_columns FROM {registeredTempTable}"
    )
    df.toDF(*[c.lower() for c in df.columns]).repartition(30).write.format(
        "org.apache.spark.sql.redis"
    ).option("table", registeredTempTable).option(
        "key.column", "namespace_id_columns"
    ).mode(
        "overwrite"
    ).save()
    # sc.toRedisKV(stringRDD)(redisConfig)


def prepare_snowflake_data(spark, snowflake_tables, parquet_path, snowflake_creds):
    # Stale tables need to pushed to Redis again
    stale_tables = []
    for table_name, query in snowflake_tables.items():
        df, stale_data = get_snowflake_df(
            spark, snowflake_creds, parquet_path, query, table_name
        )
        df.cache().registerTempTable(table_name)
        if stale_data:
            stale_tables.append(table_name)
    return stale_tables


# TODO: Superfluous call Thought this might do more
def prepare_mysql_data(spark, parquet_path, mysql_creds):
    load_mysql_df(
        spark, parquet_path, "mysql_past_24_hours_of_store_events", mysql_creds
    ).cache().registerTempTable("mysql_past_24_hours_of_store_events")


def compute_spark_aggregates(spark, spark_queries_and_table_names):
    for table_name, query in spark_queries_and_table_names.items():
        spark.sql(query).cache().registerTempTable(table_name)


def main(spark):
    # This covers up pushdown errors
    spark.sparkContext.setLogLevel("WARN")

    secrets = boto3.client("secretsmanager", region_name="us-east-1")
    snowflake_creds = json.loads(
        secrets.get_secret_value(SecretId="snowflake/analytics_api")["SecretString"]
    )
    mysql_creds = json.loads(
        secrets.get_secret_value(SecretId="rds-prod-drizly-ro")["SecretString"]
    )
    # TODO: Change these
    parquet_path = "table_saves"
    snowflake_schema = "stage_js"
    snowflake_tables = {
        "store_order_history": snowflake_store_order_history_query.format(
            snowflake_schema=snowflake_schema
        ),
        "location_time_deltas": snowflake_location_time_deltas_query.format(
            snowflake_schema=snowflake_schema
        ),
        "store_metadata": snowflake_store_metadata_query.format(
            snowflake_schema=snowflake_schema
        ),
        "eta_fallback": snowflake_eta_fallback_query.format(
            snowflake_schema=snowflake_schema
        ),
        "driver_history": snowflake_driver_delivery_history_query.format(
            snowflake_schema=snowflake_schema
        ),
    }
    stale_tables = prepare_snowflake_data(
        spark, snowflake_tables, parquet_path, snowflake_creds
    )
    redis_registeredTempTable_and_namespace_id_columns = {
        "store_order_history": ["store_id"],
        "store_metadata": ["store_id"],
        "eta_fallback": ["store_id"],
        "driver_history": ["store_id", "driver_id"],
        "location_time_deltas": ["delivery_zipcode", "dow", "hod"],
        "store_history_live": ["store_id"],
        "store_context": ["store_id"],
    }
    prepare_mysql_data(spark, parquet_path, mysql_creds)
    lookback_hours = 2
    last_n_orders_plus_one = 6
    spark_queries_and_table_names = {
        "store_orders_with_times": spark_store_orders_with_times,
        "active_drivers": spark_active_drivers.format(lookback_hours=lookback_hours),
        "active_orders": spark_active_orders.format(lookback_hours=lookback_hours),
        "store_context": spark_store_context,
        "store_history_live": spark_store_history_live.format(
            last_n_orders_plus_one=last_n_orders_plus_one
        ),
    }
    compute_spark_aggregates(spark, spark_queries_and_table_names)
    # TODO: This should only write values that are newly computed
    for (
        registeredTempTable,
        namespace_id_columns,
    ) in redis_registeredTempTable_and_namespace_id_columns.items():
        push_df_to_redis(spark, registeredTempTable, namespace_id_columns)
    for table in stale_tables + ["store_context", "store_history_live"]:
        registeredTempTable = table
        namespace_id_columns = redis_registeredTempTable_and_namespace_id_columns[table]
        push_df_to_redis(spark, registeredTempTable, namespace_id_columns)
        logging.warning(f"Pushed {table} to redis")
    for registeredTempTable in list(snowflake_tables.keys()) + [
        "mysql_past_24_hours_of_store_events"
    ]:
        spark.sql(f"SELECT * FROM {registeredTempTable}").write.mode("overwrite").save(
            f"{parquet_path}/{registeredTempTable}"
        )


# TODO: EMR request needs the following:
# [
#   {
#      "Classification": "spark-env",
#      "Configurations": [
#        {
#          "Classification": "export",
#          "Properties": {
#             "PYSPARK_PYTHON": "/usr/bin/python3"
#           }
#        }
#     ]
#   }
# ]

# def lambda_handler(event, context):
#     emr = boto3.client('emr')
#     version = 'latest'
#     main_path = join('s3://<artifacts-bucket-name>', version, 'main.py')
#     modules_path = join('s3://<artifacts-bucket-name>', version, 'module_seed.zip')
#
#     job_parameters = {
#         'job_name': '<your-job-name>',
#         'input_path': 's3://<raw-data-path>',
#         'output_path': 's3://<processed-data-path>',
#         'spark_config': {
#             '--executor-memory': '1G',
#             '--driver-memory': '2G'
#         }
#     }
#
#     step_args = [
#         "/usr/bin/spark-submit",
#         '--py-files', modules_path,
#         main_path, str(job_parameters)
#     ]
#
#     step = {
#         "Name": job_parameters['job_name'],
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
#             'Args': step_args
#         }
#     }
#
#     action = emr.add_job_flow_steps(JobFlowId='<emr-cluster-id>', Steps=[step])
#     return action

# Upon running:
# 1) load snowflake parquets x
# 2) integration_tests their size  x
# 3) replace any stale data  x
# 4) load mysql data  x
# 5) get most recent store_order_events id x
# 6) get the most recent data x
# 7) compute the values x
# 8) push up to redis  x
main(spark)
