from smv.smvgenericmodule import SmvProcessModule
from smv.smviostrategy import SmvParquetPersistenceStrategy
from smv.error import SmvRuntimeError

import utils
from dsalib.provider import DsaModuleProvider

class DsaH2oModule(SmvProcessModule, DsaModuleProvider):
    """Integration glue code definition between h2o.ai (pysparkling water) and EDDIE
    """
    @classmethod
    def help_markdown(cls):
        return """#### h2o Module

This module takes n inputs of any type and returns an h2o.H2oFrame.

Common use-case is to take an input from a pyspark module and to use h2o to do some
processing on it.

```python
from dsalib import utils
spark_df = SomeInputModule # assume this is a spark dataframe
h2oFrame = utils.sparkToH2O(spark_df)

# do some processing with h2o...
return h2oFrame
```
"""


    @staticmethod
    def provider_type():
        return 'h2o'

    @staticmethod
    def attributes():
        return ['requiresDS', 'requiresLib', 'requiresConfig', 'run', 'isEphemeral']

    def dsType(self):
        return 'Module'

    def persistStrategy(self):
        return DsaH2oAsSparkIoStrategy(
            self.smvApp, self.versioned_fqn
        )

    # TODO: need add this to SmvGenericModule so that we always check
    def _assure_output_type(self, run_output):
        # TODO move this back to top
        import h2o
        if (not isinstance(run_output, h2o.H2OFrame)):
            raise SmvRuntimeError(
                'The run method output should be an H2OFrame, but {} is given.'.format(type(run_output))
            )

    # Override doRun to assure run output is a pandas DF
    def doRun(self, known):
        # TODO move these back up to the top
        import h2o
        import pysparkling
        i = self.RunParams(known)
        res = self.run(i)
        self._assure_output_type(res)
        return res

class DsaH2oAsSparkIoStrategy(SmvParquetPersistenceStrategy):
    """TODO: This could be optimized to read/write to/from parquet directly"""
    def _read(self):
        spark_df = super(DsaH2oAsSparkIoStrategy, self)._read()
        return utils.sparkToH2o(spark_df)
    def _write(self, rawdata):
        spark_df = utils.h2oToSpark(rawdata)
        # RETURN spark df
        super(DsaH2oAsSparkIoStrategy, self)._write(spark_df)

__all__ = [
    'DsaH2oModule',
]
