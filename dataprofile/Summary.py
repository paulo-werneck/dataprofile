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

    def get_variables_segregated(self):
        type_number = ('int', 'bigint', 'float', 'decimal', 'double', 'number')
        type_string = ('string', 'varchar')
        list_number = [n for n, k in self.data_frame.dtypes if k in type_number]
        list_string = [n for n, k in self.data_frame.dtypes if k in type_string]
        return list_number, list_string

