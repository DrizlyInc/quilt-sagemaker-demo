import numpy as np
import pandas as pd

## This is in case scoring time is substantially slowing down the rate at which we select a model
## Currently unused
def interval_score_numpy_optimized(
    y, y_preds, containment_reward=5.0, underestimate_cost=-10.0, overestimate_cost=-1.0
):
    # Range from 0 - 5.0. Intervals of width 1 received the highest score
    containment_reward_calculation = containment_reward / np.fmax(
        y_preds[0, :, 1] - y_preds[0, :, 0] - 10.0, 1.0
    )
    containment_reward_sum = np.where(
        (y_preds[0, :, 0] <= y) & (y_preds[0, :, 1] >= y),
        containment_reward_calculation,
        0.0,
    ).sum()
    understimate_cost_calculation = np.fmin(
        underestimate_cost * ((y - y_preds[0, :, 1]) / y_preds[0, :, 1]) ** 2,
        underestimate_cost,
    )
    underestimate_cost_sum = np.where(
        y_preds[0, :, 1] < y, understimate_cost_calculation, 0.0
    ).sum()
    overestimate_cost_calculation = np.fmin(
        overestimate_cost * ((y_preds[0, :, 0] - y) / y_preds[0, :, 0]) ** 2,
        overestimate_cost,
    )
    overestimate_cost_sum = np.where(
        y_preds[0, :, 0] > y, overestimate_cost_calculation, 0.0
    ).sum()
    return containment_reward_sum + underestimate_cost_sum + overestimate_cost_sum


# Scoring Function
# Contained within the window, points awarded divided by width of window, capped at the value of the containment_reward
def containment_reward_func(row, containment_reward):
    return containment_reward / max(row["upper"] - row["lower"], 1.0)


# For understimation and overestimation, the worst case loss occurs when you miss by 100%
# It computes the min because the cost is a positive value that should be subtracted
def underestimate_cost_func(row, underestimate_cost):
    return min(
        underestimate_cost
        * ((row["true_value"] - row["upper"]) / max(row["upper"], 1.0)) ** 2,
        underestimate_cost,
    )


# Same as above, just a different ordering of the subtraction. Could parameterize and make the same function
# But it leaves room for different scaling of misses
def overestimate_cost_func(row, overestimate_cost):
    return min(
        overestimate_cost
        * ((row["lower"] - row["true_value"]) / max(row["lower"], 1.0)) ** 2,
        overestimate_cost,
    )


def interval_score(
    y, y_preds, containment_reward=5.0, underestimate_cost=10.0, overestimate_cost=1.0
):
    # Set up DF for applies
    df = y.to_frame(name="true_value")
    df["lower"] = y_preds[0, :, 0]
    df["upper"] = y_preds[0, :, 1]
    # Filter by containment, then score the rows that have the value, the aggregate
    containment_reward_sum = (
        df[(df["lower"] <= df["true_value"]) & (df["lower"] <= df["true_value"])]
        .apply(containment_reward_func, args=(containment_reward,), axis=1)
        .sum()
    )
    underestimate_cost_sum = (
        df[(df["upper"] < df["true_value"])]
        .apply(underestimate_cost_func, args=(underestimate_cost,), axis=1)
        .sum()
    )
    overestimate_cost_sum = (
        df[(df["lower"] > df["true_value"])]
        .apply(overestimate_cost_func, args=(overestimate_cost,), axis=1)
        .sum()
    )
    # Compute the average
    return (
        containment_reward_sum - underestimate_cost_sum - overestimate_cost_sum
    ) / len(y)