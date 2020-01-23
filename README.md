# pod-dataprofile
repositório do projeto de data profile - pod

Este repositório tem como finalidade compartilhar o projeto de dataprofile para datasets.

Input: Datasets csv, parquet, txt ou hive (a implementar)
Output: Profile gerado em PDF.

#### Classes e Funções:

* class CreateProfile (main)

        def read_file()

        def mount_tables()

        def header()

        def footer()

        def subtitle()

        def body()

        def viewer()

* class Summary - (pai)

        def columns_type()

        def volume_base()
  
* class SummaryNumber - (filha)

        def get_statistics()
  
* class SummaryString - (filha)

        def get_statistics()
