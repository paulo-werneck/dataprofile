from dataprofile.SparkConfigs import spark_session


class Summary:

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def volume_base(self):
        index = ["Colunas", "Linhas"]
        data = [(len(self.data_frame.columns), self.data_frame.count())]
        return spark_session.createDataFrame(data, index)

    def columns_types(self):
        index = ["Colunas", "Tipo"]
        data = self.data_frame.dtypes
        return spark_session.createDataFrame(data, index)

    def get_statistics(self, tuple_type):
        dtypes = self.data_frame.dtypes
        columns = [n for n, k in dtypes if k in tuple_type]
        if 'string' in tuple_type:
            return self.data_frame.select(columns).summary("count", "min", "max").toPandas().transpose()
        else:
            return self.data_frame.select(columns).summary().toPandas().transpose()
