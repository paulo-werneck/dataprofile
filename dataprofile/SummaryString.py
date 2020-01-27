from dataprofile.SparkConfigs import spark_session
from dataprofile.Summary import Summary
from pyspark.sql.functions import countDistinct, when, isnull


class SummaryString(Summary):

    def __init__(self, data_frame):
        super().__init__(data_frame)

    def get_statistics(self):
        return self.data_frame.select(self.get_variables_segregated()[1]).summary("count", "min", "max")

    def get_count_distinct(self):
        return self.data_frame.agg(*(countDistinct(c).alias(c) for c in self.get_variables_segregated()[1])). \
            toPandas().transpose()

    def get_missing(self):
        nulls = self.data_frame.select(*(when(isnull(c) == 'true', 1).otherwise(0).alias(c) for c in self.get_variables_segregated()[1]))
        agrupaded = nulls.groupBy().sum(*self.get_variables_segregated()[1])
        return agrupaded.select(*(agrupaded['sum(' + c + ')'].alias(c) for c in self.get_variables_segregated()[1]))
