"""Creating a dataset from types you already know as strings.

`_schema_to_columns` took a draken Morsel or a duck-typed relation schema, and
nothing else. A caller holding `{"name": "source_ip", "type": "IPV4"}` - which
is exactly the shape this method RETURNS, and exactly what
`create_dataset(schema={"columns": [...]})` reads as - was refused with
"Unsupported schema type", because a dict's `columns` is a key and not an
attribute.

That is not a hypothetical caller. The upload service holds its own type
vocabulary, deliberately does not depend on the query engine, and had no way to
say VARCHAR: every `create_dataset` it issued failed with a 500 at commit, after
the data had been uploaded.
"""

import pytest

from opteryx_catalog.opteryx_catalog import OpteryxCatalog

STORED = [
    {"name": "cve_id", "type": "VARCHAR"},
    {"name": "published", "type": "TIMESTAMP[us]"},
    {"name": "source_ip", "type": "IPV4"},
    {"name": "hosts", "type": "INT64"},
]


def columns_for(schema):
    return OpteryxCatalog._schema_to_columns(None, schema)


class TestStoredSpelling:
    def test_a_mapping_with_columns_is_accepted(self):
        assert [c["type"] for c in columns_for({"columns": STORED})] == [
            "VARCHAR",
            "TIMESTAMP[us]",
            "IPV4",
            "INT64",
        ]

    def test_a_bare_list_is_accepted_too(self):
        assert columns_for(STORED) == columns_for({"columns": STORED})

    def test_the_exact_name_is_stored_not_a_category(self):
        # The whole reason the quartet exists: IPV4's category is INTEGER, and
        # storing the category reads back as a signed INT64 - an address
        # rendered as a number.
        assert columns_for({"columns": [{"name": "ip", "type": "IPV4"}]})[0]["type"] == "IPV4"

    def test_ids_are_allocated_in_order(self):
        assert [c["id"] for c in columns_for({"columns": STORED})] == [1, 2, 3, 4]

    def test_field_ids_are_honoured(self):
        cols = OpteryxCatalog._schema_to_columns(None, {"columns": STORED}, field_ids=[7, 8, 9, 10])
        assert [c["id"] for c in cols] == [7, 8, 9, 10]

    def test_a_field_id_count_that_does_not_match_is_refused(self):
        with pytest.raises(ValueError, match="does not match column count"):
            OpteryxCatalog._schema_to_columns(None, {"columns": STORED}, field_ids=[1])

    def test_an_empty_column_list_is_empty_rather_than_unsupported(self):
        assert columns_for({"columns": []}) == []

    def test_a_column_with_no_type_stores_varchar(self):
        assert columns_for([{"name": "unknown"}])[0]["type"] == "VARCHAR"


class TestParametersComeOutOfTheName:
    """The name is authoritative; the quartet's other three are copies of it.

    They are separate stored columns other readers consume, so a caller that
    only spelled the name still gets a complete record.
    """

    def test_decimal_precision_and_scale(self):
        column = columns_for([{"name": "amount", "type": "DECIMAL(10, 2)"}])[0]
        assert (column["type"], column["precision"], column["scale"]) == ("DECIMAL(10, 2)", 10, 2)

    def test_an_array_element_type(self):
        column = columns_for([{"name": "tags", "type": "ARRAY<VARCHAR>"}])[0]
        assert (column["type"], column["element-type"]) == ("ARRAY<VARCHAR>", "VARCHAR")

    def test_what_the_caller_spelled_out_wins(self):
        # Evolution hands back stored dicts carrying exactly these keys.
        column = columns_for(
            [{"name": "amount", "type": "DECIMAL(10, 2)", "precision": 12, "scale": 4}]
        )[0]
        assert (column["precision"], column["scale"]) == (12, 4)

    def test_a_name_whose_parameters_do_not_read_is_still_stored_whole(self):
        # Refusing a dataset over the redundant half of the record would be the
        # wrong trade.
        column = columns_for([{"name": "odd", "type": "DECIMAL(a, b)"}])[0]
        assert column["type"] == "DECIMAL(a, b)"
        assert (column["precision"], column["scale"]) == (None, None)

    def test_a_bare_decimal_carries_no_parameters(self):
        column = columns_for([{"name": "amount", "type": "DECIMAL"}])[0]
        assert (column["type"], column["precision"]) == ("DECIMAL", None)


class TestTheOtherShapesStillWork:
    def test_a_relation_schema_like_object(self):
        class Category:
            def __init__(self, name):
                self.name = name

        class Type:
            def __init__(self, name, category):
                self._name, self.category = name, Category(category)

            def __str__(self):
                return self._name

        class Column:
            def __init__(self, name, column_type):
                self.name, self.column_type = name, column_type

        class Schema:
            def __init__(self, columns):
                self.columns = columns

        schema = Schema([Column("source_ip", Type("IPV4", "INTEGER"))])
        assert columns_for(schema)[0]["type"] == "IPV4"

    def test_something_that_is_neither_says_what_it_wanted(self):
        with pytest.raises(ValueError) as caught:
            columns_for(42)
        message = str(caught.value)
        assert "int" in message
        assert "stored spelling" in message

    def test_a_list_of_things_that_are_not_columns_is_not_mistaken_for_one(self):
        # Falls through to the duck-typed branches rather than being read as
        # stored columns with no names.
        with pytest.raises(ValueError):
            columns_for(["source_ip", "published"])
