import pytest
import csql.queries_basic as sql


class Test_Select:
    def test_select_all(self):
        assert sql.select("table") == "SELECT * FROM table"

    def test_select_specific(self):
        assert sql.select("table", ["col1, col2"]) == "SELECT col1, col2 FROM table"

    def test_select_where(self):
        assert sql.select("tbl", query_fields=["col1", "col2"]) == "SELECT * FROM tbl WHERE col1 = %s AND col2 = %s"

    @pytest.mark.parametrize("invalid_tables", [
        "",
        ["Bob"],
        {"Never odd or even"},
        ("Do geese see God?",)
        ])
    def test_select_table_type(self, invalid_tables):
        with pytest.raises(Exception):
            sql.select(invalid_tables)

    @pytest.mark.parametrize("invalid_fields", [
        "",
        "test",
        {"Never odd or even"},
        ("Do geese see God?",),
        [1, ["wow"]]
        ])
    def test_select_field_type(self, invalid_fields):
        with pytest.raises(Exception):
            sql.select("test", invalid_fields)

    @pytest.mark.parametrize("invalid_fields", [
        "",
        "test",
        {"Never odd or even"},
        ("Do geese see God?",),
        [1, ["wow"]]
        ])
    def test_select_where_field_type(self, invalid_fields):
        with pytest.raises(Exception):
            sql.select("test", query_fields=invalid_fields)


class Test_Insert:
    def test_insert(self):
        assert sql.insert("table", ["col1", "col2"]) == "INSERT INTO table (col1, col2) VALUES (%s, %s)"

    @pytest.mark.parametrize("invalid_tables", [
        "",
        ["Bob"],
        {"Never odd or even"},
        ("Do geese see God?",)
        ])
    def test_insert_table_type(self, invalid_tables):
        with pytest.raises(Exception):
            sql.insert(invalid_tables, [1, 2])

    @pytest.mark.parametrize("invalid_fields", [
        "",
        "test",
        {"Never odd or even"},
        ("Do geese see God?",),
        [1, ["wow"]]
        ])
    def test_insert_field_type(self, invalid_fields):
        with pytest.raises(Exception):
            sql.insert("test", invalid_fields)


class Test_Update:
    def test_update_where(self):
        assert sql.update("tbl", [1, 2], [3, 4]) == "UPDATE tbl SET 1 = %s, 2 = %s WHERE 3 = %s AND 4 = %s"

    def test_update(self):
        assert sql.update("tbl", [1, 2]) == "UPDATE tbl SET 1 = %s, 2 = %s"

    @pytest.mark.parametrize("invalid_tables", [
        "",
        ["Bob"],
        {"Never odd or even"},
        ("Do geese see God?",)
        ])
    def test_update_table_type(self, invalid_tables):
        with pytest.raises(Exception):
            sql.update(invalid_tables, [1])

    @pytest.mark.parametrize("invalid_fields", [
        "",
        "test",
        {"Never odd or even"},
        ("Do geese see God?",),
        [1, ["wow"]]
        ])
    def test_update_field_type(self, invalid_fields):
        with pytest.raises(Exception):
            sql.update("test", invalid_fields)

    @pytest.mark.parametrize("invalid_fields", [
        "",
        "test",
        {"Never odd or even"},
        ("Do geese see God?",),
        [1, ["wow"]]
        ])
    def test_update_where_field_type(self, invalid_fields):
        with pytest.raises(Exception):
            sql.update("test", [1, 2], query_fields=invalid_fields)
