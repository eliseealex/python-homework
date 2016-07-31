import pandas as pd


def read_queries_by_user_id(users_path, page_views_path, backend_path):
    """
    Reads source data and exposes it as a generator over users' identifiers.

    :param users_path: path to the users.csv file for current hour
    :param page_views_path: path to the Pageviews.csv file for current hour
    :param backend_path: path to the Backend.csv file for current hour
    :return: generator over the tuples (user_id: sorted_queries_data)
    """

    all_queries_enriched = _read_queries(backend_path, page_views_path, users_path)

    user_ids = all_queries_enriched['user_id']

    for user_id in user_ids.unique():
        yield user_id, all_queries_enriched[user_ids == user_id].sort_values('timestamp')


def _read_queries(backend_path, page_views_path, users_path):
    users = pd.read_csv(users_path, delimiter=";")
    page_views = pd.read_csv(page_views_path, delimiter=";")
    backend = pd.read_csv(backend_path, delimiter=";")

    all_queries = page_views.append(backend, ignore_index=True)
    all_queries_enriched = pd.merge(all_queries, users, how="left", on="user_id")

    return all_queries_enriched


if __name__ == "__main__":
    for queries in read_queries_by_user_id("data/users.csv", "data/Pageviews.csv", "data/Backend.csv"):
        print(queries[1])
