# Standard Library Imports
import logging

# Third Party Library Imports

# Local Library Imports
import src.csql.classes as classes

# Configure Logging
log = logging.getLogger('log')


def create_database(database: classes.database) -> list[str]:
    '''Generates a series of sql statements to create a new database'''
    if isinstance(database, classes.database) is False:
        raise TypeError(f"DATABASE IS NOT VALID TYPE; {type(database)}")

    response = []

    sql = f"CREATE DATABASE {database.name}"
    response.append(sql)

    for table in database.tables:
        response.extend(create_table(table))

    return response


def create_table(table: classes.table, db_name: str | None = None) -> list[str]:
    '''Generates a series of sql statements to create a new table'''

    response = []

    if db_name is None:
        sql = f"CREATE TABLE {table.name} ("
    else:
        sql = f"CREATE TABLE {db_name}.{table.name} ("

    for field in table.fields:
        log.debug(f"SQLGEN: {field}")

        sql = f"{sql}{field.name} {field.type}"
        if field.length is not None:
            sql = f"{sql}({field.length})"
        if field.increment is True:
            sql = f"{sql} AUTO_INCREMENT"
        if field.null is False:
            sql = f"{sql} NOT NULL"
        if field.primary is True:
            sql = f"{sql} PRIMARY KEY"
        sql = f"{sql}, "
    sql = f"{sql[:-2]})"

    response.append(sql)

    return response


def create_field(field: classes.field, table_name: str, db_name: str) -> list[str]:
    '''Generates a series of sql statements to create a new table'''

    log.debug(f"SQLGEN: {field}")

    response = []
    sql = f"ALTER TABLE {db_name}.{table_name} ADD COLUMN "

    sql = f"{sql}{field.name} {field.type}"
    if field.length is not None:
        sql = f"{sql}({field.length})"
    if field.increment is True:
        sql = f"{sql} AUTO_INCREMENT"
    if field.null is False:
        sql = f"{sql} NOT NULL"
    if field.primary is True:
        sql = f"{sql} PRIMARY KEY"

    response.append(sql)

    return response
