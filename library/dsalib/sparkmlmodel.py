import abc
from smv.smvgenericmodule import SmvProcessModule
from smv.smviostrategy import SmvFileOnHdfsPersistenceStrategy

from dsalib.provider import DsaModelProvider
from pyspark.ml import PipelineModel

class DsaSparkMlPipeLineModel(SmvProcessModule, DsaModelProvider):
    """Base model class for all the modules which create Spark ML models
        User need to implement:

            - mlModelClass
            - requiresDS
            - run
    """

    @staticmethod
    def provider_type():
        return 'sparkmlpipe'

    @classmethod
    def display_name(cls):
        return 'Spark ML Pipeline'

    @staticmethod
    def attributes():
        return ['requiresDS', 'requiresLib', 'requiresConfig', 'run', 'isEphemeral']

    def dsType(self):
        return 'Model'

    def persistStrategy(self):
        return DsaSparkMlPipeLineIoStrategy(
            self.smvApp, self.versioned_fqn
        )

    @classmethod
    def help_markdown(cls):
        return """#### Spark ML Pipeline Model

Take multiple input data as defined in __Dependencies__, and feed a Model as
a __pyspark.ml.PipelineModel__.

The depended module names can be used as input DataFrame directly.

The input data can be any type. User may need to convert them before fit a model.
Almost all models expect `(label:Float, feature:Vector)` type of data. For regular
Spark DF input with features as individual column, may need to _assemble_ before use.

Example:

Using Linear Regression as an example, other models are similar.

__TrainingModel: as a Spark ML Pipeline Model

```python
# TrainData is the input with columns ["label", "v1", "v2", "v3"]
assembler = pyspark.ml.feature.VectorAssembler(
    inputCols=["v1", "v2", "v3"],
    outputCol="features"
)
lr = pyspark.ml.regression.LinearRegression(
    maxIter=5,
    regParam=0.0,
    solver="normal"
)
pipeline = pyspark.ml.Pipeline(stages=[assembler, lr])
model = pipeline.fit(TrainData)
return model
```

__ScoringModule__: as a normal PySpark Module

```python
# TrainingModel and TestData are inputs
return TrainingModel.transform(TestData)
```
"""

class DsaSparkMlPipeLineIoStrategy(SmvFileOnHdfsPersistenceStrategy):
    def __init__(self, smvApp, versioned_fqn, file_path=None):
        super(DsaSparkMlPipeLineIoStrategy, self).__init__(smvApp, versioned_fqn, 'mlmod', file_path)

    def _read(self):
        return PipelineModel.read().load(self._file_path)

    def _write(self, rawdata):
        rawdata.write().overwrite().save(self._file_path)
