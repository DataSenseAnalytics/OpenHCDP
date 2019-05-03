from smv.smvgenericmodule import SmvProcessModule
from smv.smviostrategy import SmvParquetPersistenceStrategy
from smv.error import SmvRuntimeError

import pandas

from dsalib.provider import DsaModuleProvider

class DsaPandasModule(SmvProcessModule, DsaModuleProvider):
    """Base class for all the modules with returns Pandas DF
        User need to implement:

            - requiresDS
            - run

        Provider prefix: mod.module.pandas

        For better performance, user need to add the following line
        to the cluster config's "Spark Arguments":

            --conf spark.sql.execution.arrow.enabled=true
    """
    @classmethod
    def help_markdown(cls):
        return """#### Pandas DataFrame Module

Take multiple input data as defined in __Dependencies__, and perform pandas
operations. Output have to be a __Pandas DataFrame__.

The input data can be any type. User may need to convert them to Pandas DataFrame.

The depended module names can be used as input DataFrame directly.

Example:
```python
my_pdf = MyInputSparkDF.toPandas()
return my_pdf.head(10)
```

To convert a Pandas DF to Spark DF, one can use an util function
`pandasToSpark` in the code,

```python
my_spark_df = pandasToSpark(my_pandas_df)
```
"""


    @staticmethod
    def provider_type():
        return 'pandas'

    @staticmethod
    def attributes():
        return ['requiresDS', 'requiresLib', 'requiresConfig', 'run', 'isEphemeral']

    def dsType(self):
        return 'Module'

    def persistStrategy(self):
        return DsaPandasAsSparkIoStrategy(
            self.smvApp, self.versioned_fqn
        )

    # TODO: need add this to SmvGenericModule so that we always check
    def _assure_output_type(self, run_output):
        if (not isinstance(run_output, pandas.DataFrame)):
            raise SmvRuntimeError(
                'The run method output should be a Pandas DataFrame, but {} is given.'.format(type(run_output))
            )

    # Override doRun to assure run output is a pandas DF
    def doRun(self, known):
        i = self.RunParams(known)
        res = self.run(i)
        self._assure_output_type(res)
        return res

class DsaPandasAsSparkIoStrategy(SmvParquetPersistenceStrategy):
    def _read(self):
        sparkdf = super(DsaPandasAsSparkIoStrategy, self)._read()
        return sparkdf.toPandas()

    def _write(self, rawdata):
        sparkdf = self.smvApp.sparkSession.createDataFrame(rawdata)
        super(DsaPandasAsSparkIoStrategy, self)._write(sparkdf)

__all__ = [
    'DsaPandasModule',
]
