from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd

import json
import boto3


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from matplotlib import pyplot as plt
from joblib import dump

import sys
sys.path.append("..")
from app.eta.models.RandomForestInterval import RandomForestInterval
from app.eta.model_evaluation.IntervalScorer import interval_score
from app.eta.model_evaluation.IntervalVisualizer import graph_intervals
from app.eta.models.EnsembleModel import EnsembleModel

def connect_to_snowflake_alchemy(
    warehouse="REPORT", database=None, schema="TEST", role="analyst"
):
    secrets = boto3.client("secretsmanager", region_name="us-east-1")
    snowflake_creds = json.loads(
        secrets.get_secret_value(SecretId="snowflake/analytics_api")["SecretString"]
    )
    if database is None:
        database = snowflake_creds["database"]
    engine = create_engine(
        URL(
            account=snowflake_creds["account"],
            user=snowflake_creds["username"],
            password=snowflake_creds["password"],
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
        )
    )
    connection = engine.connect()
    return connection

# TODO: Needs logic for 3pd vs no 3pd, tho this could be done in pandas as a filter on eta_training
def get_training_data(target_db):
    return pd.read_sql(
        """select * FROM {target_db}.DATASCIENCE.etas_training_no_3pd limit 10000;""".format(target_db=target_db),
        con=connect_to_snowflake_alchemy(),
    ).set_index("store_order_id")

# TODO: This should become a configuration. Part of the goal is to maintain the ability to quickly change
# TODO: what columns are used to generate each portion of the model. Probably need these values to be outside
# TODO: the container itself. Possibly in s3 -> s3://drizly-ml/models/etas/target_cols_and_training_cols/modelVersion/ ...
# TODO: How do we A/b integration_tests models?
def get_target_cols_and_training_cols():
    target_cols = [
        "minutes_enroute_to_complete",
        "minutes_placed_to_accepted",
        "minutes_accepted_to_enroute",
        "delivery_minutes"
    ]
    training_cols = {
        # Features here should measure driver tendencies
        # Location/time tendencies
        "minutes_enroute_to_complete": [
            "enroute_to_complete_zipwide_waterfall",
            "minutes_enroute_to_complete_median",
            "minutes_enroute_to_complete_most_granular",
            "minutes_enroute_to_complete_most_granular_context",
            "minutes_enroute_to_complete_waterfall",
            "marking_enroute_poorly"
        ],
        # Features here should measure administrative load
        "minutes_placed_to_accepted": [
            "is_a_driver_at_store",
            "pct_capacity",
            "minutes_placed_to_accepted_median",
            "minutes_placed_to_accepted_most_granular",
            "minutes_placed_to_accepted_waterfall"
        ],
        # Features here should measure administrative load as well as driver states
        "minutes_accepted_to_enroute": [
            "is_a_driver_at_store",
            "pct_capacity",
            "minutes_accepted_to_enroute_median",
            "marking_enroute_poorly",
            "driver_minutes_accepted_to_enroute_diff",
            "minutes_accepted_to_enroute_median_week_diff",
            "minutes_accepted_to_enroute_median_day_diff",
            "minutes_accepted_to_enroute_median_hour_diff",
            "minutes_accepted_to_enroute_wsum_last_n_completed_orders_diff",
        ],
        "delivery_minutes":[
            "is_a_driver_at_store",
            "driver_minutes_accepted_to_enroute_diff",
            "pct_capacity",
            "marking_enroute_poorly",
            "driver_delivery_minutes_diff",
            "minutes_placed_to_accepted_median",
            "minutes_accepted_to_enroute_median",
            "minutes_enroute_to_complete_median",
            "minutes_enroute_to_complete_most_granular_context",
            "delivery_minutes_median_week_diff",
            "delivery_minutes_median_day_diff",
            "delivery_minutes_median_hour_diff",
            "delivery_minutes_wsum_last_n_completed_orders_diff",
            "delivery_minutes_median",
            "delivery_minutes_most_granular",
            "delivery_minutes_waterfall",
        ]
    }
    return target_cols, training_cols

# TODO: Train thru suite of models for each of the 3 things to estimate
# Test Train split has to be all the same!
def test_models(training_data, target_col, training_cols_arr, training_size, param_grid, visualize):
    # Append store order id for indexing purposes with results
    # Drop null values.
    # TODO: Manage imputing by specific columns
    training_cols = [target_col] + training_cols_arr
    print(training_cols)
    df_training = training_data[training_cols].dropna()
    X = df_training.drop([target_col], axis=1)
    y = df_training[target_col]
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=training_size)
    # best_model, best_params, best_score = cross_validate(X_train, y_train, param_grid)
    # TODO: Make ^ This work, to remove everything from here to y_interval_predicted below.
    n_estimators_to_try = [100]
    max_depth_to_try = [2,5,None]
    best_score = None
    for n_estimators in n_estimators_to_try:
        for max_depth in max_depth_to_try:
            intervalForest = RandomForestInterval(n_jobs=-1, n_estimators=n_estimators, verbose=0, confidence_interval_lower=40,confidence_interval_upper=95)
            intervalForest.fit(X_train, y_train)
            y_preds = intervalForest.predict(X_test)
            this_score = interval_score(y_test, y_preds)
            print("" + str(this_score))
            if best_score is None or this_score > best_score:
                best_score = this_score
                best_estimators = n_estimators
                best_depth = max_depth
                best_model = intervalForest
                if visualize:
                    graph_intervals(
                    X_test,
                    y_test,
                    best_model,
                    "y_actual",
                    "lower_bound",
                    "upper_bound",
                    "80% Confidence Interval",
                    target_col,
                    "n_estimators: " + str(best_estimators) + " max_depth: " + str(best_depth),
                    n=100
                )
    X_shap = None
    if visualize:
        plt.close()
        feat_importances = pd.Series(best_model.forest.feature_importances_, index=X_train.columns)
        fig = feat_importances.nlargest(20).plot(kind='barh').get_figure()
        # fig.tight_layout()
        fig.savefig(target_col + "_feature_importance", bbox_inches = "tight")
        plt.close()
        X_shap = X_test.sample(n=1000)
    return best_model, X_shap

#TODO: Populate Health Check in S3 or in /opt/ml/model/
def write_health_check_request(health_check_row, health_check_write_directory="/opt/ml/model/"):
    request_keys=[
        "store_id",
        "delivery_zipcode",
        "dow",
        "hod"
    ]
    health_check_row[request_keys].to_csv(f"{health_check_write_directory}health-check-data.csv")


def train_model(df, target_column, training_columns, training_size, params_list=[{}]):
    training_cols = [target_column] + training_columns
    print(training_cols)
    df_training = df[training_cols].dropna()
    X = df_training.drop([target_column], axis=1)
    y = df_training[target_column]
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=training_size)
    best_model = None
    best_score = None
    #TODO: Make this able to handle more condensed parameters. If every value had to be in array form, a cross join of the
    # parameters would permit denser configuration files.
    for params in params_list:
        intervalForest = RandomForestInterval(**params)
        #
        intervalForest.fit(X_train, y_train)
        if len(params_list) > 1:
            y_preds = intervalForest.predict(X_test)
            this_score = interval_score(y_test, y_preds)
            print(f"{params}={this_score}")
            if best_score is None or this_score > best_score:
                best_model = intervalForest
                best_score = this_score
        else:
            best_model = intervalForest
    #Return X_test in case shapley values are desired
    return best_model, X_test



def train(df, target_cols, training_cols, training_size, params_list, model_write_directory="/opt/ml/model/",visualize=False):
    models_composed = EnsembleModel()
    for target_col in target_cols:
        training_columns = training_cols[target_col]
        if target_col == "delivery_minutes":
            ## This can be any model type that can process the vars.
            models_composed.add_ensemble_estimator(RandomForestInterval(**{"confidence_interval_lower": 40, "confidence_interval_upper":90}))
            models_composed.fit(df, target_col, training_columns)
        else:
            best_model, X_shap = train_model(df, target_col, training_columns, training_size, params_list)
            models_composed.add_model(target_col, training_columns, best_model)
    dump(models_composed, model_write_directory + "composed_model.pkl")
    return df.sample(n=1)

# Needs to write to s3
if __name__ == "__main__" :
    ## TODO: Should be a configuration read
    # Intriguingly, making the models not attempt to run in parallel makes it predict faster
    param_grid = [
        {"n_estimators": 30, "n_jobs": 1
        , "confidence_interval_lower": 10, "confidence_interval_upper":90}
    ]
    training_size = 0.95
    target_cols, training_cols = get_target_cols_and_training_cols()
    #TODO: Should be derived from an environment variable and or an arg
    df = get_training_data("STAGE_JS")
    print("got training data")
    #TODO: Change this to write to /opt/ml/model/ for prod √
    health_check_row = train(df, target_cols, training_cols, training_size, param_grid, model_write_directory="/opt/ml/model/")
    print("trained")
    write_health_check_request(health_check_row, health_check_write_directory="/opt/ml/model/")
    print("done")
