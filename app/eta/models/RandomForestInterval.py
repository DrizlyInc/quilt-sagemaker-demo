import numpy as np
from sklearn.base import BaseEstimator
from sklearn.utils.validation import check_X_y, check_is_fitted
from joblib import Parallel, delayed
from sklearn.ensemble import RandomForestRegressor
from sklearn.utils.fixes import _joblib_parallel_args
from sklearn.ensemble._base import _partition_estimators

#TODO: Needs a load model function

def _accumulate_prediction(predict, X, minimum_value=None):
    """
    This is a utility function for joblib's Parallel.
    It can't go locally in ForestClassifier or ForestRegressor, because joblib
    complains that it cannot pickle it when placed there.
    """
    prediction = predict(X, check_input=False)
    if minimum_value is not None:
        prediction[prediction < minimum_value] = minimum_value
    return prediction

class RandomForestInterval(BaseEstimator):
    def __init__(
        self,
        n_estimators=100,
        criterion="mse",
        max_depth=None,
        min_samples_split=2,
        min_samples_leaf=1,
        min_weight_fraction_leaf=0.,
        max_features="auto",
        max_leaf_nodes=None,
        min_impurity_decrease=0.,
        min_impurity_split=None,
        bootstrap=True,
        oob_score=False,
        n_jobs=None,
        random_state=None,
        verbose=0,
        warm_start=False,
        ccp_alpha=0.0,
        max_samples=None,
        confidence_interval_lower=10.0,
        confidence_interval_upper=90.0,
        minimum_value=0.0
    ):
        self.confidence_interval_lower = confidence_interval_lower
        self.confidence_interval_upper = confidence_interval_upper
        self.forest = RandomForestRegressor(
        n_estimators,
        criterion,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        min_weight_fraction_leaf,
        max_features,
        max_leaf_nodes,
        min_impurity_decrease,
        min_impurity_split,
        bootstrap,
        oob_score,
        n_jobs,
        random_state,
        verbose,
        warm_start,
        ccp_alpha,
        max_samples)
        self.n_estimators = n_estimators
        self.n_jobs = n_jobs
        self.verbose = verbose
        # self.n_outputs_ = 2
        self.minimum_value = minimum_value


    def fit(self, X, y, **kwargs):

        # Check that X and y have correct shape
        X, y = check_X_y(X, y)
        self.forest = self.forest.fit(X, y, **kwargs)
        self.estimators_ = self.forest.estimators_
        # Return the classifier
        self.is_fitted = True
        return self


    def _validate_X_predict(self, X):
        """
        Validate X whenever one tries to predict, apply, predict_proba."""
        check_is_fitted(self)

        return self.estimators_[0]._validate_X_predict(X, check_input=True)

    def predict(self, X):
        """
        Predict regression target for X.
        The predicted regression target of an input sample is computed as the
        mean predicted regression targets of the trees in the forest.
        Parameters
        ----------
        X : array-like or sparse matrix of shape (n_samples, n_features)
            The input samples. Internally, its dtype will be converted to
            ``dtype=np.float32``. If a sparse matrix is provided, it will be
            converted into a sparse ``csr_matrix``.
        Returns
        -------
        y : array-like of shape (n_samples,) or (n_samples, n_outputs)
            The predicted values.
        """
        check_is_fitted(self)
        # Check data
        X = self._validate_X_predict(X)

        # Assign chunk of trees to jobs
        n_jobs, _, _ = _partition_estimators(self.n_estimators, self.n_jobs)


        # Parallel loop
        # Store the output of every estimator in order to compute confidence intervals
        y_hat = Parallel(n_jobs=self.n_jobs, verbose=self.verbose,
                 **_joblib_parallel_args(require="sharedmem"))(
            delayed(_accumulate_prediction)(e.predict, X,    self.minimum_value)
            for e in self.forest.estimators_)

        y_hat_below = np.percentile(y_hat, self.confidence_interval_lower, axis=0)
        y_hat_above = np.percentile(y_hat, self.confidence_interval_upper, axis=0)

        return np.dstack((y_hat_below,y_hat_above))
