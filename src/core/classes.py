# Standard Library Imports
from dataclasses import dataclass

# Third Party Library Imports

# Local Library Imports

# Configure Logging


@dataclass
class field():
    name: str
    type: str
    length: int = None
    default: str = None
    null: bool = True
    primary: bool = False
    increment: bool = False


@dataclass
class table():
    name: str
    fields: list = None

    def lookup(self, field_name: str) -> field:
        '''Looks for the field within the db, if found, returns the field object'''

        for field in self.fields:
            if field.name == field_name:
                return field

        return


@dataclass
class database():
    name: str
    tables: list = None

    def lookup(self, table_name: str) -> table:
        '''Looks for the table within the db, if found, returns the table object'''

        for table in self.tables:
            if table.name == table_name:
                return table

        return
