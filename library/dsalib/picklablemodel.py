import smv
from dsalib.provider import DsaModelProvider

class DsaPicklableModel(smv.SmvModel, DsaModelProvider):
    """Wrapper of SmvModel as a provider

        Provider FQN: mod.model.picklable
    """

    @classmethod
    def help_markdown(cls):
        return """#### Python Object Model

Take multiple input data as defined in __Dependencies__, and calculate a Model as
a __Pickable Python Object__.

The input data can be any type. User may need to convert them and collect to create
the output object.

The depended module names can be used as input DataFrame directly.

Example:
```python
X = Features.toPandas().values

# Model training
model = sklearn.cluster.KMeans(n_clusters=3, random_state=0).fit(X)

return model
```
"""

    @staticmethod
    def provider_type():
        return 'picklable'

    @staticmethod
    def attributes():
        return ['requiresDS', 'requiresLib', 'requiresConfig', 'run', 'isEphemeral']

    @classmethod
    def display_name(cls):
        return 'Python Object Model'
