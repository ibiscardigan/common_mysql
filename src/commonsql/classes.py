# Standard Library Imports

# Third Party Library Imports

# Local Library Imports

# Configure Logging

types = [
    "VARCHAR",
    "INT",
    "FLOAT",
    "BOOL",
    "DATE",
    "DATETIME",
    "TIMESTAMP"
]


class field():
    def __init__(self,
                 name: str,
                 field_type: str,
                 length: int | None = None,
                 default: str | None = None,
                 null: bool = True,
                 primary: bool = False,
                 increment: bool = False) -> None:

        self.name = name
        if field_type.upper() in types:
            self.type = field_type.upper()
        else:
            raise ValueError(f"FIELD TYPE UNRECOGNISED; {type(field_type)}")

        if length is None or isinstance(length, int):
            self.length = length
        else:
            raise TypeError(f"FIELD LENGTH NOT INT; {type(length)}")

        self.default = default

        if isinstance(primary, bool) is False:
            raise TypeError(f"FIELD PRIMARY NOT BOOL; {type(primary)}")
        else:
            self.primary = bool(primary)

        if isinstance(null, bool) is False:
            raise TypeError(f"FIELD NULL NOT BOOL; {type(null)}")
        else:
            self.null = bool(null)
        if self.primary is True and self.null is True:
            raise ValueError("FIELD PRIMARY AND NULL CANNOT BOTH BE TRUE")

        if isinstance(increment, bool) is False:
            raise TypeError(f"FIELD INCREMENT NOT BOOL; {type(increment)}")
        else:
            self.increment = bool(increment)
        if self.type != "INT" and self.increment is True:
            raise ValueError("FIELD INCREMENT CANNOT BE TRUE IF TYPE != INT")

        pass

    def dict(self):
        '''Returns the attributes as a dict'''
        response = {}
        for attr, val in self.__dict__.items():
            response[attr] = val

        return response


class table():
    def __init__(self, name: str, fields: list[field] = []) -> None:
        self.name = name
        self.fields = []

        if isinstance(fields, list) is False:
            raise TypeError(f"FIELD SUBMISSION IS NOT LIST; {type(fields)}")

        for new_field in fields:
            if isinstance(new_field, field) is True:
                self.fields.append(new_field)
            else:
                raise TypeError(f"FIELDS ARE NOT ALL TYPE FIELD; {type(new_field)}")
        pass

    def lookup(self, field_name: str) -> field | None:
        '''Looks for the field within the db, if found, returns the field object'''

        for field in self.fields:
            if field.name == field_name:
                return field
        return None

    def dict(self):
        '''Returns the attributes as a dict'''
        response = {}
        for attr, val in self.__dict__.items():
            response[attr] = val

        return response

    def add_field(self, new_field: field) -> None:
        if isinstance(new_field, field) is True:
            self.fields.append(new_field)
        else:
            raise TypeError(f"NEW FIELD IS NOT A FIELD OBJECT; {type(new_field)}")

        return


class database():
    def __init__(self, name: str, tables: list[table] = []) -> None:
        self.name = name
        self.tables = []

        if isinstance(tables, list) is False:
            raise TypeError(f"TABLES SUBMISSION IS NOT LIST; {type(tables)}")

        for new_table in tables:
            if isinstance(new_table, table) is True:
                self.tables.append(new_table)
            else:
                raise TypeError(f"TABLES ARE NOT ALL TYPE TABLE; {type(new_table)}")
        pass

    def lookup(self, table_name: str) -> table | None:
        '''Looks for the table within the db, if found, returns the table object'''

        for table in self.tables:
            if table.name == table_name:
                return table

        return None

    def dict(self):
        '''Returns the attributes as a dict'''
        response = {}
        for attr, val in self.__dict__.items():
            response[attr] = val

        return response

    def add_table(self, new_table: table) -> list[table]:
        if isinstance(new_table, table) is True:
            self.tables.append(new_table)
        else:
            raise TypeError(f"NEW TABLE IS NOT A TABLE OBJECT; {type(new_table)}")

        return self.tables


class schema():
    def __init__(self) -> None:
        self.databases = []
        pass

    def lookup(self, database_name: str) -> database | None:
        '''Looks for the table within the db, if found, returns the table object'''

        for database in self.databases:
            if database.name == database_name:
                return database

        return None
