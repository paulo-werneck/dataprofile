from dataprofile.SparkConfigs import spark_session
from dataprofile.SummaryNumber import SummaryNumber
from dataprofile.SummaryString import SummaryString
from dataprofile.Summary import Summary
from prettytable import PrettyTable, FRAME
from fpdf import FPDF


class CreateProfile(FPDF):

    def __init__(self, client, path='../infopreco.csv', header1=True, delimiter=';', encoding=['UTF-8', 'ISO-8859-1'],
                 quote='"', escape='\\', infer_schema=True):
        super().__init__()
        self.client = client
        self.path = path
        self.header1 = header1  # se possui header no arquivo
        self.sep = delimiter
        self.encoding = encoding
        self.quote = quote
        self.escape = escape
        self.infer_schema = infer_schema
        self.base = str(path).split('.')[-2].split('/')[-1]
        self.type_file = str(path).split('.')[-1].rstrip(" ")
        self.summary = Summary(self.read_file())
        self.summary_number = SummaryNumber(self.read_file())
        self.summary_string = SummaryString(self.read_file())
        self.viewer()

    def read_file(self):
        if self.type_file == 'csv':
            return spark_session.read.csv(path=self.path, header=self.header1,
                                          sep=self.sep, encoding=self.encoding[1],
                                          quote=self.quote, escape=self.escape,
                                          inferSchema=self.infer_schema)
        elif self.type_file == 'parquet':
            return spark_session.spark.read.parquet(self.path)
        elif self.type_file == 'txt':
            return spark_session.read.text(self.path)
        elif self.type_file == 'table':
            pass
            # implementation to hive

    @staticmethod
    def mount_tables(data_frame):
        list_collect = data_frame.collect()

        # columns
        a = list_collect[0].asDict()
        b = PrettyTable(a.keys())

        # rows
        for i in list_collect:
            b.add_row(i.asDict().values())
        b.vrules = FRAME
        b.float_format = '.2f'
        b.align = 'l'
        b.right_padding_width = 1

        return b

    def header(self):
        # load logo PoD
        self.image(name='../images/SmallLogo.png', w=30, h=20)
        # set title configs
        self.set_font(family='Times', style='B', size=17)
        self.set_xy(x=90, y=20)
        self.cell(w=30, h=10, txt='Relatório de Profile', border=0, ln=0, align='C')
        self.line(x1=5, y1=30, x2=200, y2=30)
        self.ln(15)

    def footer(self):
        self.set_xy(x=-15, y=-15)
        self.set_font(family='Arial', size=8)
        self.cell(w=0, h=10, txt='Page ' + str(self.page_no()), border=0, ln=0, align='C')

    def subtitle(self):
        self.set_font(family='Arial', style='', size=11)
        self.cell(w=0, h=5, txt=f'Cliente: {self.client}', border=0, ln=1)

    def body(self):
        self.set_left_margin(margin=10)
        self.set_font(family='Courier', style='', size=9)
        self.cell(w=0, h=6, txt=f'Dataset: {self.base}', border=0, ln=1)

        # Volume
        self.set_font(family='Courier', style='B', size=9)
        self.cell(w=0, h=10, txt=f'1. Volumetria', border=0, ln=1)

        self.set_font(family='Courier', style='', size=8)
        self.multi_cell(w=0, h=5, align='L', txt=str(self.mount_tables(self.summary.volume_base())), border=0)

        # Data types
        self.set_font(family='Courier', style='B', size=9)
        self.cell(w=0, h=10, txt=f'2. Campos e tipos de dados', border=0, ln=1)

        self.set_font(family='Courier', style='', size=8)
        self.multi_cell(w=0, h=5, align='L', txt=str(self.mount_tables(self.summary.columns_types())), border=0)

        # String Variables
        self.set_font(family='Courier', style='B', size=9)
        self.cell(w=0, h=10, txt=f'3. Variáveis quantitativas', border=0, ln=1)

        self.set_font(family='Courier', style='', size=8)
        self.multi_cell(w=0, h=5, align='L', txt=str(self.mount_tables(self.summary_number.get_statistics())), border=0)

        # Number Variables
        self.set_font(family='Courier', style='B', size=9)
        self.cell(w=0, h=10, txt=f'4. Variáveis qualitativas', border=0, ln=1)

        self.set_font(family='Courier', style='', size=8)
        self.multi_cell(w=0, h=5, align='L', txt=str(self.mount_tables(self.summary_string.get_statistics())), border=0)

    def viewer(self):
        self.add_page()
        self.subtitle()
        self.body()
        self.output(f'../report/POD-profile-{self.base}.pdf', 'F')


if __name__ == "__main__":

    pdf = CreateProfile(client=' XPTO INDÚSTRIA DE ALIMENTOS SA')
