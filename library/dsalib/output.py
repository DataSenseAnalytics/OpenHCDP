from smv.conn import SmvHdfsConnectionInfo
from smv.iomod import SmvCsvOutputFile, SmvHiveOutputTable, SmvJdbcOutputTable
from dsalib.provider import DsaOutputProvider


class DsaCsvOutputModule(SmvCsvOutputFile, DsaOutputProvider):
    """Output to CSV file to publish dir with module FQN as file name

        Provider fqn prefix: mod.output.csv
    """

    @classmethod
    def help_markdown(cls):
        return """#### PySpark DataFrame Output as CSV

Take a single input data as defined in __Dependency__ as a PySpark DataFrame,
and output as a CSV file under the specified connection`
"""

    @staticmethod
    def connectionType():
        return 'hdfs'

    @staticmethod
    def provider_type():
        return 'csv'

    @staticmethod
    def attributes():
        return ['connectionName', 'requiresDSSingle', 'fileName']

    # default file name which can be overwrite by users
    def fileName(self):
        return "{}.csv".format(self.fqn())

class DsaHiveOutputModule(SmvHiveOutputTable, DsaOutputProvider):
    """Output to Hive table

        Provider fqn prefix: mod.output.hive
    """

    @classmethod
    def help_markdown(cls):
        return """#### PySpark DataFrame Output as Hive Table

Take a single input data as defined in __Dependency__ as a PySpark DataFrame,
and output as a hive table under the specified connection
"""

    @staticmethod
    def connectionType():
        return 'hive'

    @staticmethod
    def provider_type():
        return 'hive'

    @staticmethod
    def attributes():
        return ['connectionName', 'requiresDSSingle', 'tableName', 'writeMode']

class DsaJDBCOutputModule(SmvJdbcOutputTable, DsaOutputProvider):
    """Output to JDBC table

        Provider fqn prefix: mod.output.jdbc
    """

    @classmethod
    def help_markdown(cls):
        return """#### PySpark DataFrame Output as JDBC Table

Take a single input data as defined in __Dependency__ as a PySpark DataFrame,
and output as a JDBC table under the specified connection
"""

    @staticmethod
    def connectionType():
        return 'jdbc'

    @staticmethod
    def provider_type():
        return 'jdbc'

    @staticmethod
    def attributes():
        return ['connectionName', 'requiresDSSingle', 'tableName', 'writeMode']
