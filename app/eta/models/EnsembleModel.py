import logging
# logging.basicConfig(level=logging.DEBUG)
# logging = logging.getlogging()
from joblib import Parallel, delayed
import multiprocessing

# Can't be in a static context for parallelism
def _individual_model_predict(model, df, model_training_columns, model_name):
    logging.debug(f"running:{model_name}")
    return (model_name, model.predict(df[model_training_columns]))


class EnsembleModel():
    def __init__(self):
        self.dict_of_models = {}
        self.ensemble_training_columns = None
        self.ensemble_estimator = None

    def add_model(self, model_name, model_training_columns, model):
        self.dict_of_models[model_name] = (model_training_columns, model)

    def add_ensemble_estimator(self, model):
        self.ensemble_estimator = model

    #Parallelization does not appear to achieve meaningful speedup
    def parallel_pre_predict(self, df):
        num_cores = multiprocessing.cpu_count()
        parallel_processes = list()
        logging.debug(f"{num_cores}")
        for model_name, (model_training_columns, model) in self.dict_of_models.items():
            parallel_processes.append((model, df, model_training_columns, model_name))
        results = Parallel(n_jobs=num_cores)(delayed(_individual_model_predict)(*i) for i in parallel_processes)
        return results

    def fit(self, df, final_target_col, final_training_cols):
        #TODO: Throw out NA values
        #Align df length
        training_column_full_list = final_training_cols + [final_target_col]
        for model_name, (model_training_columns, model) in self.dict_of_models.items():
            training_column_full_list += model_training_columns
        df_no_na = df[list(set(training_column_full_list))].dropna()
        # Serial version
        for model_name, (model_training_columns, model) in self.dict_of_models.items():
            df_training = df_no_na[model_training_columns]
            predictions = model.predict(df_training)
            # df_no_na[f"{model_name}_predict"] = predictions[0]
            df_no_na[f"{model_name}_lower"] = predictions[0, :, 0]
            df_no_na[f"{model_name}_upper"] = predictions[0, :, 1]
            # final_training_cols.append(f"{model_name}_predict")
            final_training_cols.append(f"{model_name}_lower")
            final_training_cols.append(f"{model_name}_upper")
        training_cols = [final_target_col] + final_training_cols
        logging.info(f"Training Columns: {training_cols}")
        df_training = df_no_na[final_training_cols + [final_target_col]]
        X = df_training.drop([final_target_col], axis=1)
        y = df_training[final_target_col]
        self.ensemble_estimator.fit(X, y)
        self.ensemble_training_columns = final_training_cols


    def predict(self, df_request):
        for model_name, (model_training_columns, model) in self.dict_of_models.items():
            try:
                val = model.predict(df_request[model_training_columns])
                lower_bound = val[0][0][0]
                upper_bound = val[0][0][1]
            #TODO: Remove this try catch block for prod code or figure out a way to put it on just for debugging
            except ValueError as e:
                logging.error(f"{model_name}")
                logging.error(f"{df_request[model_training_columns].isna().any()}")
                logging.error(f"{df_request[model_training_columns].to_dict('records')}")
                raise e
            # df_request[model_name + '_predict'] = val
            df_request[model_name + "_lower"] = lower_bound
            df_request[model_name + "_upper"] = upper_bound
        # results = self.parallel_pre_predict(df_request)
        # for (model_name, predictions) in results:
        #     df_request[f"{model_name}_lower"] = predictions[0, :, 0]
        #     df_request[f"{model_name}_upper"] = predictions[0, :, 1]
        final_prediction = self.ensemble_estimator.predict(df_request[self.ensemble_training_columns])
        lower_bound_composed, upper_bound_composed = final_prediction[0][0][0], final_prediction[0][0][1]
        return lower_bound_composed, upper_bound_composed