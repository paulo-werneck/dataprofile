from dataprofile.SparkConfigs import spark_session


class Summary:

    TYPE_NUMBER = ('int', 'bigint', 'float', 'decimal', 'double', 'number')
    TYPE_STRING = ('string', 'varchar')

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

    def get_statistics(self):
        dtypes = self.data_frame.dtypes
        return [n for n in dtypes if Summary.TYPE_NUMBER in ]
