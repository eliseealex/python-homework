from basemetric import BaseMetricRegistry
from reader import read_queries_by_user_id
# Done for metric initialization see __init__.py
# noinspection PyUnresolvedReferences
from metrics import *
import pandas as pd


def calculate_metrics(users_path, page_views_path, backend_path):
    """
    Returns dataframe with metric values by user id measured based on specified source files

    :param users_path: path to csv file with users
    :param page_views_path: path to csv file with page views
    :param backend_path: path to csv file with backend queries
    :return: data frame with users ids as indexes and metrics as columns
    """

    metrics_values_by_user_id = {}

    queries_by_user_id = read_queries_by_user_id(users_path, page_views_path, backend_path)

    for user_id, queries_dataframe in queries_by_user_id:
        metrics_values_by_user_id[user_id] = BaseMetricRegistry.apply_active_metrics(queries_dataframe)

    return pd.DataFrame(metrics_values_by_user_id).transpose()


def apply_metrics(users_path, page_views_path, backend_path, metric_value_path):
    """
    Applies metrics and saves results to csv file

    :param users_path: path to csv file with users
    :param page_views_path: path to csv file with page views
    :param backend_path: path to csv file with backend queries
    :param metric_value_path: path to csv file to save metric results to
    """

    metric_values = calculate_metrics(users_path,
                                      page_views_path,
                                      backend_path)

    metric_values.to_csv(metric_value_path, index_label="user_id", sep=";")


def apply_metrics_in_dir(base_directory):
    """
    Applies metrics and saves results to csv file.
    Takes source data from specified directory with default names:
    `users.csv` for user information;
    `Pageviews.csv` for log of page views;
    `Backend.csv` for log of backend queries;

    Saves results to `metric_values.csv` in the same directory.

    :param base_directory: path to the directory to get data from, assumed that data files have default names.
    """

    apply_metrics(base_directory + "/users.csv",
                  base_directory + "/Pageviews.csv",
                  base_directory + "/Backend.csv",
                  base_directory + "/metric_values.csv")


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] == "help"):
        print("Specify directory to get data from. You will find results in the same directory.")
    elif len(sys.argv) == 2:
        apply_metrics_in_dir(sys.argv[1])
