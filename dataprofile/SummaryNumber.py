from dataprofile.SparkConfigs import spark_session
from dataprofile.Summary import Summary
from pyspark.sql.functions import countDistinct, isnull, when


class SummaryNumber(Summary):

    def __init__(self, data_frame):
        super().__init__(data_frame)

    def get_summary_spark(self):
        a = self.data_frame.select(self.get_variables_segregated()[0]).summary(). \
            toPandas()
        a = a.set_index('summary')
        a = a.transpose()
        a = a.reset_index()
        return spark_session.createDataFrame(a).\
            withColumnRenamed('index', 'Variavel')

    def get_count_distinct(self):
        a = self.data_frame.agg(*(countDistinct(c).alias(c) for c in self.get_variables_segregated()[0])). \
            toPandas().transpose()
        final = a.reset_index()
        return spark_session.createDataFrame(final).\
            withColumnRenamed('index', 'Variavel').\
            withColumnRenamed('0', 'distinct')

    def get_missing(self):
        nulls = self.data_frame.select(*(when(isnull(c) == 'true', 1).otherwise(0).alias(c) for c in self.get_variables_segregated()[0]))
        agrupaded = nulls.groupBy().sum(*self.get_variables_segregated()[0])
        final = agrupaded.select(*(agrupaded['sum(' + c + ')'].alias(c) for c in self.get_variables_segregated()[0])). \
            toPandas().transpose()
        final = final.reset_index()
        return spark_session.createDataFrame(final).\
            withColumnRenamed('index', 'Variavel').\
            withColumnRenamed('0', 'missing')

    def get_statistics(self):
        summary = self.get_summary_spark().alias('summary')
        distinct = self.get_count_distinct().alias('distinct')
        missing = self.get_missing().alias('missing')

        join = summary.join(distinct, summary['Variavel'] == distinct['Variavel']).\
            join(missing, summary['Variavel'] == missing['Variavel']).\
            drop(distinct['Variavel']).\
            drop(missing['Variavel'])

        return join