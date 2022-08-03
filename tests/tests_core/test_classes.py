import pytest
import src.core.classes as classes

expected_field = classes.field(
    name="test",
    type="VARCHAR",
    length=128,
    default=None,
    null=True,
    primary=False,
    increment=False
)


class Test_field:
    def test_field(self):
        assert classes.field("test", "VARCHAR", 128) == expected_field

    @pytest.mark.parametrize("invalid_field", [
        classes.field(name="test", type="VARCHAR", increment=True), # Should not be able to increment VARCHAR
        classes.field(name="test", type="INT", null=True, primary=True, increment=True) # Should not be able to null a primary
        ])
    def test_invalid_fielde(self, invalid_field):
        with pytest.raises(Exception):
            invalid_field
