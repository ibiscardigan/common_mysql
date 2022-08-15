# Standard Library Imports
import configparser
import logging
import pytest
from typing import Any

# Third Party Library Imports
import mysql.connector

# Local Library Imports
import src.commonsql.classes as classes

# Configure Logging
log = logging.getLogger('log')


@pytest.mark.skip
class connection:  # pragma: no cover
    def __init__(self, config: configparser.ConfigParser) -> None:
        self.host = config['mysql']['host']
        self.user = config['mysql']['user']
        self.password = config['mysql']['password']
        self.connect()
        pass

    def connect(self, database_name: str | None = None) -> None:
        '''Connects to the msql db instance'''

        if database_name is not None and isinstance(database_name, str) is False:
            raise TypeError(f"DATABASE: database_name != str; {type(database_name)}")

        if database_name is None:
            log.info(f"DATABASE: ATTEMPTING CONNECTION AS {self.user}")
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password
                    )

            except Exception as error:
                print(error)
                log.error(f"DATABASE: COULD NOT CONNECT | {error}")

        else:
            log.info(f"DATABASE: CONNECTING TO {database_name} USING {self.user}")
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=database_name
                    )
            except Exception as error:
                log.error(f"DATABASE: COULD NOT CONNECT | {error}")
                return

    def disconnect(self):  # pragma: no cover
        log.debug("DATABASE: DISCONNECTING")
        self.connection.disconnect()
        log.info("DATABASE: DISCONNECTED")
        pass

    def standard_query(self, statement: str, query_params: list[Any] = []) -> list[tuple[Any]]:
        ''''''

        if isinstance(statement, str) is False:
            raise TypeError(f"DATABASE: statement IS NOT STR; {type(statement)}")

        if isinstance(query_params, list) is False:
            raise TypeError(f"DATABASE: query_params IS NOT LIST; {type(query_params)}")

        self.cursor = self.connection.cursor()

        if len(query_params) == 0:
            self.cursor.execute(statement)
        else:
            query_params = tuple(query_params)  # type: ignore
            self.cursor.execute(statement, query_params)

        result = self.cursor.fetchall()
        self.cursor.close()

        return list(result)

    def standard_execute(self, statement: str, query_params: list[Any] = []) -> int:

        if isinstance(statement, str) is False:
            raise TypeError(f"DATABASE: statement IS NOT STR; {type(statement)}")

        if isinstance(query_params, list) is False:
            raise TypeError(f"DATABASE: query_params IS NOT LIST; {type(query_params)}")

        self.cursor = self.connection.cursor()
        query_params = tuple(query_params)  # type: ignore
        self.cursor.execute(statement, query_params)

        self.connection.commit()

        response = self.cursor.rowcount
        self.cursor.close()

        return int(response)

    def schema_execute(self, statement: str) -> None:

        if isinstance(statement, str) is False:
            raise TypeError(f"DATABASE: statement IS NOT STR; {type(statement)}")

        self.cursor = self.connection.cursor()
        self.cursor.execute(statement)

        self.connection.commit()

        self.cursor.close()

        return

    def build_schema(self) -> list:
        '''Queries a MySQL instance and returns a list of the database with their structure as dict'''

        self.cursor = self.connection.cursor()

        try:
            self.databases = self.get_databases()
        except Exception as error:
            log.critical(f"DATABASE: {error}")

        self.cursor.close()

        return self.databases

    def lookup(self, database_name: str) -> classes.database:
        '''Looks for a database in the schema, and if found returns it'''
        for db in self.databases:
            if db.name == database_name:
                return db

        return

    def get_databases(self) -> list:
        '''Queries a MySQL instance and returns a list of the databases'''

        sql = "SHOW DATABASES"
        log.info(f"DATABASE: QUERY: {sql}")

        try:
            self.cursor.execute(sql)
        except Exception as error:
            log.critical(f"DATABASE: {error}")

        results = self.cursor.fetchall()
        log.info(f"DATABASE: RESPONSE: {results}")

        response = []
        default_databases = ["information_schema", "mysql", "performance_schema"]

        for record in results:
            if record[0] not in default_databases:
                database = classes.database(name=record[0])
                log.info(f"DATABASE: PROCESSING {database.name}")

                self.use_database(database_name=database.name)

                database.tables = self.get_tables()
                response.append(database)
                log.info(f"DATABASE: PROCESSED: {database}")

        return response

    def use_database(self, database_name: str) -> None:
        '''Changes the database being queried'''

        sql = f"USE {database_name}"
        log.info(f"DATABASE: QUERY: {sql}")
        try:
            self.cursor.execute(sql)
        except Exception as error:
            log.critical(f"DATABASE: {error}")
            sys.exit()

        return

    def get_tables(self) -> list:
        '''Takes in a database name and queries for the tables'''
        sql = "SHOW TABLES"

        log.info(f"DATABASE: QUERY: {sql}")
        self.cursor.execute(sql)

        results = self.cursor.fetchall()
        log.debug(f"DATABASE: RESPONSE: {results}")

        response = []
        for record in results:
            table_record = classes.table(name=record[0])
            log.info(f"DATABASE: PROCESSING TABLE: {table_record.name}")
            table_record.fields = self.get_fields(table_name=table_record.name)
            response.append(table_record)

        log.info(f"DATABASE; PROCESSED {len(response)} TABLES")

        return response

    def get_fields(self, table_name: str) -> list:
        '''Queries for the fields in a table'''
        sql = f"SHOW COLUMNS FROM {table_name}"

        log.info(f"DATABASE: QUERY: {sql}")
        self.cursor.execute(sql)

        results = self.cursor.fetchall()
        log.debug(f"DATABASE: RESPONSE: {results}")

        response = []
        for record in results:
            log.debug(f"DATABASE: PROCESSING FIELD: {record[0]}")
            field_type = record[1].replace("(", ",").replace(")", "").split(",")

            field_data = classes.field(
                name=record[0],
                type=field_type[0].upper()
            )
            if len(field_type) > 1:
                if field_type[1].isnumeric() is True:
                    field_data.length = int(field_type[1])
                else:
                    field_data.length = int(field_type[1][:-9])

            if record[2] == "NO":
                field_data.null = False
            else:
                field_data.null = True

            if "PRI" in record[3]:
                field_data.primary = True
            else:
                field_data.primary = False

            if "auto_increment" in record[5]:
                field_data.increment = True
            else:
                field_data.increment = False

            response.append(field_data)

        log.info(f"DATABASE: PROCESSED {len(response)} FIELDS")

        return response
