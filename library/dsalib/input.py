from smv.iomod import SmvCsvInputFile, SmvHiveInputTable, SmvJdbcInputTable, SmvMultiCsvInputFiles
from dsalib.provider import DsaInputProvider


class DsaHiveInputModule(SmvHiveInputTable, DsaInputProvider):
    """Hive input table wraps SmvHiveInputTable
        User need to provide "connectionName" and "tableName"
        Provider fqn: mod.input.hive
    """
    @classmethod
    def help_markdown(cls):
        return """#### Read in Hive table as PySpark DataFrame

Read in a Hive table from system configured Hive store. Assume the Spark
configuration which EDDiE run on has Hive configures.

Output a PySpark DataFrame
"""

    @staticmethod
    def connectionType():
        return 'hive'

    @staticmethod
    def provider_type():
        return 'hive'

    @staticmethod
    def attributes():
        return ['tableName', 'connectionName']

class DsaJdbcInputModule(SmvJdbcInputTable, DsaInputProvider):
    """Hive input table wraps SmvJdbcInputTable
        User need to provide "connectionName" and "tableName"
        Provider fqn: mod.input.jdbc
    """
    @classmethod
    def help_markdown(cls):
        return """#### Read in JDBC table as PySpark DataFrame

Read in a JDBC table from the specified connection.

Output a PySpark DataFrame
"""

    @staticmethod
    def connectionType():
        return 'jdbc'

    @staticmethod
    def provider_type():
        return 'jdbc'

    @staticmethod
    def attributes():
        return ['tableName', 'connectionName']

class DsaCsvInputFileModule(SmvCsvInputFile, DsaInputProvider):
    """Csv input file wraps SmvCsvInputFile
        User need to provide "connectionName" and "fileName"
    """

    @classmethod
    def help_markdown(cls):
        return """#### Read in CSV file as PySpark DataFrame

Read in a CSV file under the specified connection path

Output a PySpark DataFrame
    """

    @staticmethod
    def connectionType():
        return 'hdfs'

    @staticmethod
    def provider_type():
        return 'csv'

    @staticmethod
    def attributes():
        return ['fileName', 'userSchema', 'connectionName', 'failAtParsingError']

class DsaMultiCsvInputFilesModule(SmvMultiCsvInputFiles, DsaInputProvider):
    """Csv input file wraps SmvMultiCsvInputFiles
        User need to provide "connectionName" and "fileName"
    """

    @classmethod
    def help_markdown(cls):
        return """#### Read in CSV files as PySpark DataFrame

Read in multiple CSV files with the same schema under the specified directory,
which is relative to the path defined in the connection. E.g. the daily updates
stored as sepreate CSV files.

Output a PySpark DataFrame
    """

    # we use fileName() to store the value of dirName
    def dirName(self):
        return self.fileName()

    @staticmethod
    def connectionType():
        return 'hdfs'

    @staticmethod
    def provider_type():
        return 'multiCsv'

    @staticmethod
    def attributes():
        return ['dirName','fileName', 'userSchema', 'connectionName', 'failAtParsingError']
