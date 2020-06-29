import pandas as pd
import matplotlib as plt
import numpy as np
import shap
#TODO: This needs to write to s3 if the goal is to evaluate model drift over time

def graph_intervals(
    X_test,
    y,
    best_model,
    actual_label,
    lower_bound_label,
    upper_bound_label,
    interval_label,
    title,
    model_params,
    n=100,
    # x_axis_col="store_order_id",
):
    # Don't show plots.
    y_interval_predicted = best_model.predict(X_test)
    X = X_test.copy()
    pd.options.mode.chained_assignment = None
    X[lower_bound_label] = y_interval_predicted[0, :, 0]
    X[upper_bound_label] = y_interval_predicted[0, :, 1]
    # X[lower_bound_label] = best_model.forest.predict(X_test)
    # X[upper_bound_label] = best_model.forest.predict(X_test)
    X[actual_label] = y
    pd.options.mode.chained_assignment = "warn"
    plt.rcParams["figure.figsize"] = [9, 16]
    plt.rcParams["figure.dpi"] = 400
    fig = plt.figure()
    X_sample_w_index = X.sample(n=n).sort_values(by=[actual_label])
    X_sample = X_sample_w_index.reset_index()
    plt.plot(
    X_sample.index,
    X_sample[actual_label],
    "b.",
    markersize=10,
    label=u"Observations"
    )
    plt.plot(X_sample.index, X_sample[lower_bound_label], "k-")
    plt.plot(X_sample.index, X_sample[upper_bound_label], "k-")
    plt.fill(
        np.concatenate([X_sample.index, X_sample.index[::-1]]),
        np.concatenate(
            [X_sample[upper_bound_label], X_sample[lower_bound_label][::-1]]
        ),
        alpha=0.2,
        fc="b",
        ec="None",
        label=interval_label,
    )
    contains = (
        len(
            X[
                (X[lower_bound_label] <=   X[actual_label])
                & (X[actual_label] <=   X[upper_bound_label])
            ].index
        )
        / len(X.index)
    )
    over_estimates = len(X[(X[lower_bound_label] > X[actual_label])].index) / len(X.index)
    under_estimates = len(X[(X[upper_bound_label] < X[actual_label])].index) / len(X.index)
    median_interval_width = (X[upper_bound_label] - X[lower_bound_label]).median()
    score = interval_score(y, y_interval_predicted)
    scores = (
        r"contains={0:.0%}"
        + "\n"
        + r"underestimates={1:.0%}"
        + "\n"
        + r"overestimates={2:.0%}"
        + "\n"
        + r"med_interval_width={3:.2f}"
        + "\n"
        + r"score={4:.2f}"
    ).format(contains, over_estimates, under_estimates, median_interval_width, score)
    extra = plt.Rectangle(
        (0, 0), 0, 0, fc="w", fill=False, edgecolor="none", linewidth=0
    )
    plt.legend([extra], [scores], loc="upper left")
    plt.title(title + ":" + model_params)
    plt_filename = title + ":" + model_params + ".png"
    plt.savefig(plt_filename, bbox_inches = "tight")
    plt.close(fig)
    return plt_filename


def show_shap_vals(model, df):
    explainer = shap.TreeExplainer(model.forest)
    shap_values = explainer.shap_values(df)
    shap.summary_plot(shap_values, df, show=False)