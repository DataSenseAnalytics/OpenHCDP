import abc
from smv.provider import SmvProvider

class DsaModProvider(SmvProvider):
    """Base provider for all modules dsalib provides
    """
    @staticmethod
    def provider_type():
        return 'mod'

    @classmethod
    def display_name(cls):
        """As a human readable name of the this mod provider
            Default to us the base provider_type, but final provider class
            can override
        """
        return cls.provider_type()

    @staticmethod
    def language():
        """The language of the Mod code
            Default to use python, but final provider class can override
        """
        return 'python'

    @classmethod
    def help_markdown(cls):
        """Provide a help markdown string for UI to show user how to use
            this type of module. For example, what to put in run method
        """
        return ''

    @abc.abstractmethod
    def attributes():
        # Need to be @staticmethod when define in concrete class
        pass

    @staticmethod
    def connectionType():
        return None

class DsaModuleProvider(DsaModProvider):
    """Base provider for all modules

        Provider fqn prefix: mod.module
    """
    @staticmethod
    def provider_type():
        return 'module'


class DsaModelProvider(DsaModProvider):
    """Base provider for all models

        Provider fqn prefix: mod.model
    """
    @staticmethod
    def provider_type():
        return 'model'

class DsaInputProvider(DsaModProvider):
    """Base provider for all input modules

        Provider fqn prefix: mod.input
    """
    @staticmethod
    def provider_type():
        return 'input'


class DsaOutputProvider(DsaModProvider):
    """Base provider for all output modules

        Provider fqn prefix: mod.output
    """
    @staticmethod
    def provider_type():
        return 'output'

