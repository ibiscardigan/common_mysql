import pytest
import src.csql.classes as classes
import src.csql.queries_schema as sql

test_field = classes.field(name="test_field", field_type="INT", null=False, primary=True, increment=True)
fsql = f"{test_field.name} {test_field.type} AUTO_INCREMENT NOT NULL PRIMARY KEY"
test_field_2 = classes.field(name="test_field_2", field_type="VARCHAR", length=128)
fsql2 = f"{test_field_2.name} {test_field_2.type}({test_field_2.length})"
test_table = classes.table(name="test_table", fields=[test_field])
test_database = classes.database(name="test_db")


class Test_databases:
    def test_create_database_invalid_name(self):
        with pytest.raises(TypeError):
            sql.create_database("test")

    def test_create_database(self):
        assert sql.create_database(test_database) == [f"CREATE DATABASE {test_database.name}"]

    def test_multiple_tables_database(self):
        test_database.add_table(test_table)
        assert sql.create_database(test_database) == [f"CREATE DATABASE {test_database.name}",
                                                      f"CREATE TABLE {test_table.name} ({fsql})"]


class Test_table():
    def test_create_table(self):
        assert sql.create_table(test_table) == [f"CREATE TABLE {test_table.name} ({fsql})"]

    def test_create_table_2(self):
        test_table.add_field(test_field_2)
        assert sql.create_table(test_table) == [f"CREATE TABLE {test_table.name} ({fsql}, {fsql2})"]

    def test_table_with_database(self):
        test_table.fields = [test_field]
        assert sql.create_table(test_table, "db") == [f"CREATE TABLE db.{test_table.name} ({fsql})"]


class Test_field():
    def test_create_field(self):
        assert sql.create_field(test_field, "table", "db") == [f"ALTER TABLE db.table ADD COLUMN {fsql}"]

    def test_create_field_2(self):
        assert sql.create_field(test_field_2, "table", "db") == [f"ALTER TABLE db.table ADD COLUMN {fsql2}"]
