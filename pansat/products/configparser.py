"""
pansat.products.configparser
============================

A parser for config files describing data products.

"""
from configparser import ConfigParser
import json

class InconsistentDimensionsError(Exception):
    pass

class UnknownTypeError(Exception):
    pass

class MissingFieldError(Exception):
    pass

class Dimension:
    def __init__(self, name, config_dict):
        if not "name" in config_dict:
            raise MissingFieldError(f"No field 'name' in section for dimensions {name}")
        self.name = name
        self.field_name = config_dict["name"]
        self._size = None

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, value):
        if not self._size:
            self._size = value
        else:
            if not self._size == value:
                raise InconsistentDimensionsError("Deduced inconsistent"
                                                  " dimensions for dimension"
                                                  f" {self.name}.")

    def __repr__(self):
        return f"Dimension({self.name})"

class VariableBase:

    def _parse_config_dict(self, config_dict):
        if not "name" in config_dict:
            raise MissingFieldError(f"Dimension  definition for '{self.name}' has no "
                                  "'name' field.")
        self.field_name = config_dict["name"]
        if not "dimensions" in config_dict:
            raise MissingFieldError(f"Dimension  definition for '{self.name}' has no "
                                  "'dimensions' field.")
        self.dimensions = json.loads(config_dict["dimensions"])
        self.unit = config_dict.get("unit", "")
        self.description = config_dict.get("description", "")


    def __init__(self, name, config_dict):
        self.name = name
        self._parse_config_dict(config_dict)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

class Coordinate(VariableBase):
    def __init__(self, name, config_dict):
        super().__init__(name, config_dict)

class Variable(VariableBase):
    def __init__(self, name, config_dict):
        super().__init__(name, config_dict)

class Attribute(VariableBase):
    def __init__(self, name, config_dict):
        super().__init__(name, config_dict)

class ProductFileDescription(ConfigParser):

    def __init__(self, filename):
        super().__init__()
        self.dimensions = []
        self.coordinates = []
        self.variables = []
        self.attributes = []
        self.read(filename)
        self._parse_config_file()

    def _parse_config_file(self):
        for section_name in self.sections():
            section = self[section_name]
            if not "type" in section:
                raise MissingFieldError(f"Section {section_name} has no 'type'"
                                            " field.")
            section_type = section["type"].lower()
            if section_type == "dimension":
                self.dimensions.append(Dimension(section_name, section))
            elif section_type == "coordinate":
                self.coordinates.append(Coordinate(section_name, section))
            elif section_type == "variable":
                self.variables.append(Variable(section_name, section))
            elif section_type == "attribute":
                self.attributes.append(Attribute(section_name, section))
            else:
                raise UnknownTypeError("Type should be one of ['dimension', "
                                       "'coordinate', 'variable'] but is "
                                       f"'{section_type}'.")
