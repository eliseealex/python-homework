from abc import ABCMeta, abstractmethod
from inspect import isabstract


class MetricRegistryMeta(ABCMeta):
    """
    This metaclass adds every concrete subclass to the registry.
    """

    def __init__(cls, name, bases, nmspc):
        super(MetricRegistryMeta, cls).__init__(name, bases, nmspc)
        if not hasattr(cls, 'registry'):
            cls.registry = set()

        if not isabstract(cls):
            cls.registry.add(cls())

    def __iter__(cls):
        return iter(cls.registry)

    def __str__(cls):
        if cls in cls.registry:
            return cls.__name__
        return cls.__name__ + ": " + ", ".join([sc.__name__ for sc in cls])


class DuplicateMetricKeyError(BaseException):
    def __init__(self, metric_key):
        super().__init__(self)

        self.metric_key = metric_key

    def __str__(self):
        return "Duplicate key in metric registry: %s" % self.metric_key


class BaseMetricRegistry(object, metaclass=MetricRegistryMeta):
    @abstractmethod
    def is_active(self):
        """
        Returns whether metric is active. If it isn't active, results will be ignored.
        """
        pass

    @abstractmethod
    def calculate_metrics(self, each_grouped_pandas_df):
        """
        Returns dictionary of metric names to values measured on specified data frame

        :param each_grouped_pandas_df: data frame to measure metrics on
        """
        pass

    @staticmethod
    def apply_active_metrics(each_grouped_pandas_df):
        """
        Returns dictionary with values for all active metrics
        :param each_grouped_pandas_df: dataframe to measure metrics on
        """

        metric_values = {}

        for metric in BaseMetricRegistry:

            if not metric.is_active():
                continue

            metric_results = metric.calculate_metrics(each_grouped_pandas_df)

            for metric_name, metric_value in metric_results.items():
                if metric_name in metric_values:
                    raise DuplicateMetricKeyError(metric_name)

                metric_values[metric_name] = metric_value

        return metric_values
