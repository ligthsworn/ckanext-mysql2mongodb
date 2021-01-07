from abc import ABC, abstractmethod

class AbstractSchemaConversion(ABC):

    @abstractmethod

    def set_config(self, schema_conv_init_option, schema_conv_output_option):
        pass

    def run(self):
        pass

    def get(self):
        pass