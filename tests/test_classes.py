import pytest
import commonsql.classes as classes

expected_field = classes.field(
    name="test",
    field_type="VARCHAR",
    length=128,
    default=None,
    null=True,
    primary=False,
    increment=False
)

fdict = {'name': 'test', 'type': 'VARCHAR', 'length': 128, 'default': None, 'primary': False, 'null': True, 'increment': False}

expected_table = classes.table(
    name="test",
    fields=[expected_field]
)

expected_database = classes.database(
    name="test",
    tables=[expected_table]
)


class Test_field:
    def test_field(self):
        test_field = classes.field(name="test", field_type="VARCHAR", length=128)
        assert test_field.name == expected_field.name
        assert test_field.type == expected_field.type
        assert test_field.length == expected_field.length
        assert test_field.default == expected_field.default
        assert test_field.null == expected_field.null
        assert test_field.primary == expected_field.primary
        assert test_field.increment == expected_field.increment

    def test_dict_output(self):
        assert expected_field.dict() == fdict

    def test_invalid_field_increment_nonint(self):
        with pytest.raises(ValueError):
            classes.field(name="test", field_type="VARCHAR", increment=True)  # Should not be able to increment VARCHAR

    def test_invalid_field_null_primary(self):
        with pytest.raises(ValueError):
            classes.field(name="test", field_type="INT", null=True, primary=True, increment=True)  # Cant null a primary

    def test_invalid_field_type(self):
        with pytest.raises(ValueError):
            classes.field(name="test", field_type="TEST")

    def test_invalid_field_length(self):
        with pytest.raises(TypeError):
            classes.field(name="test", field_type="INT", length="test")

    def test_invalid_field_null(self):
        with pytest.raises(TypeError):
            classes.field(name="test", field_type="INT", null="test")

    def test_invalid_field_increment(self):
        with pytest.raises(TypeError):
            classes.field(name="test", field_type="INT", increment="test")

    def test_invalid_field_primary(self):
        with pytest.raises(TypeError):
            classes.field(name="test", field_type="INT", primary="test")


class Test_table:
    def test_table(self):
        test_table = classes.table("test", [expected_field])
        assert test_table.name == expected_table.name
        assert test_table.fields[0].length == expected_table.fields[0].length

    def test_add_field(self):
        expected_table.add_field(expected_field)
        assert expected_table.fields[0].length == expected_field.length

    def test_invalid_add_field(self):
        with pytest.raises(TypeError):
            expected_table.add_field("test")

    def test_invalid_fields(self):
        with pytest.raises(TypeError):
            classes.table("test", "test")

    def test_invalid_field(self):
        with pytest.raises(TypeError):
            classes.table("test", ["test"])

    def test_dict_output(self):
        expected_table.fields = []
        assert expected_table.dict() == {'name': 'test', 'fields': []}

    def test_field_lookup_null(self):
        assert expected_table.lookup(expected_field.name) is None

    def test_field_lookup(self):
        expected_table.fields = [expected_field]
        assert expected_table.lookup(expected_field.name).length == expected_field.length


class Test_database:
    def test_database(self):
        test_database = classes.database("test", [expected_table])
        assert test_database.name == expected_database.name
        assert test_database.tables[0].fields[0].length == expected_field.length

    def test_add_table(self):
        expected_database.add_table(expected_table)
        assert expected_database.tables[0].fields[0].length == expected_field.length

    def test_invalid_add_table(self):
        with pytest.raises(TypeError):
            expected_database.add_table("test")

    def test_invalid_table(self):
        with pytest.raises(TypeError):
            classes.database("test", "test")

    def test_invalid_tables(self):
        with pytest.raises(TypeError):
            classes.database("test", ["test"])

    def test_dict_output(self):
        expected_database.tables = []
        assert expected_database.dict() == {'name': 'test', 'tables': []}

    def test_table_lookup_null(self):
        assert expected_database.lookup(expected_table.name) is None

    def test_field_lookup(self):
        expected_database.tables = [expected_table]
        assert expected_database.lookup(expected_table.name).fields[0].length == expected_field.length
