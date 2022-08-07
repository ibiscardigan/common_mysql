# Standard Library Imports
import configparser
import logging
import pytest
from typing import Any

# Third Party Library Imports
import mysql.connector

# Local Library Imports

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
