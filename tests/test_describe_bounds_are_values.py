"""`describe()` must report a column's BOUNDS, not their ordinal keys.

Manifest min/max are int64 ORDINAL KEYS produced by `draken_ordinalize` -- one
monotonic encoding per type so a single typed array can carry a whole
relation's mixed bounds. Nothing reversed that, so `describe()` published the
key: `planets.mass`, a DECIMAL(6,1) whose real maximum is 1898.0, described
itself as 18980; `planets.name` as 2499701452822282240; a column with no bound
as -9223372036854775808. Those numbers flow straight out to OData's
`Custom.Statistics.Min/Max` and to the Studio, where they are read as values.

The expected keys below are HARD-CODED on purpose. Computing them here with
the same helper the code uses would assert only that a function is its own
inverse -- true of a wrong pair as easily as a right one. These literals came
from `draken`'s own `ColumnType.ordinalize`, so they pin this module to the
encoding in `draken/ops/ordinalize.h` rather than to itself: if that kernel's
encoding changes, this fails instead of silently decoding to nonsense.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.dataset import _from_ordinal
from opteryx_catalog.catalog.dataset import _is_text_type
from opteryx_catalog.catalog.dataset import _ordinal_text_prefix

ORDINAL_NULL = -(2**63)


def test_integers_are_their_own_ordinal():
    assert _from_ordinal(42, "INT64") == 42
    assert _from_ordinal(-9007199254740993, "INT64") == -9007199254740993
    assert _from_ordinal(-7, "INT32") == -7
    assert _from_ordinal(4000000000, "UINT32") == 4000000000


def test_uint64_is_unbiased():
    """UINT64 is the one integer width whose range does not fit int64, so it is
    biased by the sign bit; read raw it reports a large positive as negative."""
    assert _from_ordinal(9223372036854775192, "UINT64") == 18446744073709551000


def test_decimal_is_the_unscaled_mantissa():
    """The bug the user saw: a DECIMAL bound off by a factor of 10**scale."""
    assert _from_ordinal(18980, "DECIMAL(6, 1)") == 1898.0
    assert _from_ordinal(-335, "DECIMAL(6, 1)") == -33.5
    assert _from_ordinal(0, "DECIMAL(6, 1)") == 0.0
    # No scale in the spelling means no rescale, not a guessed default.
    assert _from_ordinal(18980, "DECIMAL") == 18980


def test_floats_decode_on_both_sides_of_zero():
    """Negative floats have all 63 non-sign bits flipped so signed int64 order
    matches value order. Reinterpreting the bits without undoing that gives a
    number of roughly the right magnitude and entirely the wrong value, which
    is the failure mode most likely to pass a careless eye."""
    assert _from_ordinal(4609434218613702656, "FLOAT64") == 1.5
    assert _from_ordinal(-4643512921809643111, "FLOAT64") == -273.15
    assert _from_ordinal(-118622047889322842, "FLOAT64") == -1e-300
    assert _from_ordinal(0, "FLOAT64") == 0.0
    assert _from_ordinal(-4612811918334230529, "FLOAT32") == -2.5


def test_booleans_come_back_as_booleans():
    assert _from_ordinal(1, "BOOLEAN") is True
    assert _from_ordinal(0, "BOOLEAN") is False


def test_no_bound_is_not_a_bound():
    """ORDINAL_NULL means "no non-null value here". Published as a number it
    reads as a real, absurd minimum -- and it is the int64 minimum, so it wins
    every comparison downstream."""
    for spelling in ("INT64", "FLOAT64", "DECIMAL(6, 1)", "VARCHAR", "TIMESTAMP"):
        assert _from_ordinal(ORDINAL_NULL, spelling) is None
    assert _ordinal_text_prefix(ORDINAL_NULL) is None


def test_text_bounds_are_never_published_as_values():
    """A text ordinal is the first eight bytes shifted, so it recovers a PREFIX.
    A prefix is a sound lower bound and an UNSOUND upper one, so it must not be
    `max`."""
    assert _from_ordinal(2499701452822282240, "VARCHAR") is None
    for spelling in ("VARCHAR", "NVARCHAR", "VARBINARY", "VARIANT", "BLOB"):
        assert _is_text_type(spelling)
        assert _from_ordinal(2499701452822282240, spelling) is None
    assert not _is_text_type("INT64")
    assert not _is_text_type("TIMESTAMP")


def test_text_prefixes_are_readable():
    """What the display channel gets instead: seven exact bytes. The eighth
    lost its low bit to the shift and is dropped rather than reported off by
    one, so a sixteen-character value yields seven characters, not eight."""
    assert _ordinal_text_prefix(2499701452822282240) == "Earth"
    assert _ordinal_text_prefix(2682659064654248192) == "Jupiter"
    assert _ordinal_text_prefix(3494793310839504896) == "a"
    assert _ordinal_text_prefix(3508640226122871732) == "abcdefg"
    # A multi-byte character straddling the seven-byte cut is replaced, not
    # raised: the display channel is cosmetic and must not cost a column a row.
    assert _ordinal_text_prefix(7049425002381292215) == "é-acce"


def test_an_undeclared_type_is_not_a_text_type():
    """`_stored_type_display` renders a column document with no `type` as
    VARCHAR -- a rendering fallback, not a claim that the column is text.
    Reading it as one drops bounds that untyped documents still carry
    correctly, which `test_describe_ragged_stats` pins from the other side."""
    assert _from_ordinal(5, None) == 5
    assert _from_ordinal(5, "") == 5
    assert not _is_text_type(None)
    assert not _is_text_type("")


def test_a_non_ordinal_bound_passes_through():
    """Bounds written before the ordinal encoding, or stored as text, are
    values already. Reinterpreting one would corrupt it."""
    assert _from_ordinal("Earth", "VARCHAR") == "Earth"
    assert _from_ordinal(1.5, "FLOAT64") == 1.5
    assert _from_ordinal(None, "INT64") is None


if __name__ == "__main__":  # pragma: no cover
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
