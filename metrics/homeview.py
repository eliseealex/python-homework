from basemetric import BaseMetricRegistry


class HomeViewMetric(BaseMetricRegistry):
    def calculate_metrics(self, each_grouped_pandas_df):
        pages_history = each_grouped_pandas_df.page_name.dropna()

        home_page = (pages_history == "home.htm")

        solutions_after_home = (home_page.shift(1) == (pages_history == "solutions.htm"))

        return {"home views": home_page.sum(),
                "solutions_after_home": solutions_after_home.sum()}

    def is_active(self):
        return True
