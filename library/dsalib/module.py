from smv import SmvModule, SmvSqlModule
from dsalib.provider import DsaModuleProvider


class DsaPySparkModule(SmvModule, DsaModuleProvider):
    """Wraps SmvModule

        Provider fqn: mod.module.pyspark
    """
    @classmethod
    def help_markdown(cls):
        return """#### PySpark Module

Take multiple input data as defined in __Dependencies__, and perform standard PySpark DataFrame
operations and output a single __PySpark DataFrame__.

The depended module names can be used as input PySpark DataFrame(s) directly.

`pyspark.sql.functions` is imported as `F`, which can be used in code.

Example:
```python
return MyInput1\\
    .join(MyInput2, ['user_id'], 'inner')\\
    .withColumn('name',
        F.concat(F.col('first_name'), F.col('last_name'))
    )
```
"""

    @staticmethod
    def provider_type():
        return 'pyspark'

    @staticmethod
    def attributes():
        return ['requiresDS', 'run', 'requiresLib', 'requiresConfig', 'isEphemeral']

class DsaSparkSqlModule(SmvSqlModule, DsaModuleProvider):
    """Wraps SmvSqlModule

        provider fqn: mod.module.sparksql
    """
    @classmethod
    def help_markdown(cls):
        return """#### Spark SQL Module

Take multiple input data as defined in __Dependencies__, and perform SQL operations
which spark-sql supports. Input data have to be Spark DataFrame(s).

The depended module names can be used as input table directly.

Example:
```sql
select * from MyInput
```
"""


    @staticmethod
    def provider_type():
        return 'sparksql'

    @staticmethod
    def attributes():
        return ['tables', 'query', 'requiresConfig', 'isEphemeral']

    @staticmethod
    def language():
        return 'sql'

