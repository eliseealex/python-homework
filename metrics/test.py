from basemetric import BaseMetricRegistry


class FirstMetric(BaseMetricRegistry):
    def calculate_metrics(self, each_grouped_pandas_df):
        return {"test": 1}

    def is_active(self):
        return False
