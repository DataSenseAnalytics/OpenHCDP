import abc
import os
from smv.smvgenericmodule import SmvProcessModule
from smv.smviostrategy import SmvFileOnHdfsPersistenceStrategy

from dsalib.provider import DsaModelProvider
import utils

class DsaH2oModel(SmvProcessModule, DsaModelProvider):
    """Base model class for all the modules which create H2O ML models
        User need to implement:

            - mlModelClass
            - requiresDS
            - run
    """

    @staticmethod
    def provider_type():
        return 'H2O_ML'

    @classmethod
    def display_name(cls):
        return 'H2O ML'

    @staticmethod
    def attributes():
        return ['requiresDS', 'requiresLib', 'requiresConfig', 'run', 'isEphemeral']

    def dsType(self):
        return 'Model'

    def persistStrategy(self):
        return DsaH2oMLIoStrategy(
            self.smvApp, self.versioned_fqn
        )

    @classmethod
    def help_markdown(cls):
        return """#### H2O Model

Note: Need to install __H2O PySparkling__(`pip install h2o_pysparkling_2.3`)
in order to use H2O model and modules

Take multiple input data as defined in __Dependencies__, and feed a Model as
a __H2O Model__.

The depended module names can be used as input DataFrame directly, while may need
to converted to H2O Frame if not.

The input data can be any type. User may need to convert them before fit a model.

Example:

Using Gradient Boosting Machine as an example, other models are similar.

__TrainingModel__: as a H2O Model
```python
# TrainData is the input with columns ["ID", "CAPSULE", "AGE", "RACE", "DPROS",
# "DCAPS", "PSA", "VOL", "GLEASON"]
from h2o.estimators.gbm import H2OGradientBoostingEstimator
# convert sparkDf to h2oDf if necessary
h2o_df = dsalib.utils.sparkToH2o(TrainData)
# Convert the response column to a factor
h2o_df["CAPSULE"] = h2o_df["CAPSULE"].asfactor()
# Generate a GBM model using the training dataset
model = H2OGradientBoostingEstimator(distribution="bernoulli",
                                     ntrees=100,
                                     max_depth=4,
                                     learn_rate=0.1)
model.train(y="CAPSULE", x=["AGE","RACE","PSA","GLEASON"],training_frame=h2o_df)
return model
```

__PredictionModule__: as a H2O Module

```python
# TrainingModel and TestData are inputs
# convert sparkDf to h2oDf if necessary
h2o_df = dsalib.utils.sparkToH2o(TestData)
predict = TrainingModel.predict(h2o_df)
return predict
```
"""

class DsaH2oMLIoStrategy(SmvFileOnHdfsPersistenceStrategy):
    def __init__(self, smvApp, versioned_fqn, file_path=None):
        super(DsaH2oMLIoStrategy, self).__init__(smvApp, versioned_fqn, 'H2O_ML', file_path)

    def _read(self):
        # need to init h2o context so as to use h2o.load_model
        utils.getH2oContext()
        # Load the first file under _file_path which should be the model file.
        # We can do this because if we change the model file, the hash of hash would change
        # the _file_path and then we would save the model under different path.
        h2o_model_file = os.listdir(self._file_path)[0]
        h2o_model_path = os.path.join(self._file_path, h2o_model_file)
        import h2o
        return h2o.load_model(h2o_model_path)

    def _write(self, rawdata):
        # need to init h2o context so as to use h2o.save_model
        utils.getH2oContext()
        # Note save_model() generates a H2O model file under the given path, rather than
        # using the given path as the fileName
        import h2o
        h2o.save_model(model=rawdata, path=self._file_path, force=True)
