# flake8: noqa: I101

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from dynamicio.inject import InjectionError, check_injections, inject


def test_inject():
    res = inject("hello [[ world ]]", world="there")
    assert res == "hello there"


def test_inject_does_not_inject_nones():
    res = inject("hello [[ world ]]", world=None)
    assert res == "hello [[ world ]]"


def test_inject_does_passes_none_input():
    res = inject(None, world=None)
    assert res == None


def test_inject_with_path():
    res = inject(Path("hello [[ world ]]"), world="there")
    assert res == Path("hello there")


def test_inject_kwargs_is_case_insensitive():
    res = inject("hello [[ world ]]", WORLD="there")
    assert res == "hello there"


def test_inject_value_is_case_insensitive():
    res = inject("hello [[ WOrLD ]]", world="there")
    assert res == "hello there"


def test_inject_matches_multiple():
    res = inject("[[ VAR1 ]]/[[VAR2]]", var1="hello", var2="there")
    assert res == "hello/there"


def test_inject_various_data_types():
    res = inject(
        "[[ VAR1 ]]/[[ VAR2 ]]/[[ VAR3 ]]/[[ VAR4 ]]/[[ VAR5 ]]/[[ VAR6 ]]",
        var1=1,
        var2=[1, 2, 3],
        var3={"hello": "there"},
        var4=34.98,
        var5=datetime(2021, 1, 1),
        var6=np.array([1, 2, 3]),
    )
    assert res == "1/[1, 2, 3]/{'hello': 'there'}/34.98/2021-01-01 00:00:00/[1 2 3]"


def test_inject_accepts_extra():
    res = inject("[[ VAR1 ]]", var1="hello", var2="there", var3="extra")
    assert res == "hello"


def test_inject_curly():
    res = inject("hello {world }", world="there")
    assert res == "hello there"


def test_inject_works_correctly_with_multiple_some_not_injected():
    result = inject("[[ VAR1 ]]/[[ VAR2 ]]/{VAR3 }", var2="there")
    assert result == "[[ VAR1 ]]/there/{VAR3 }"


def test_check_injections_throws_on_missing_var():
    with pytest.raises(InjectionError):
        result = inject("[[ VAR1 ]]/[[ VAR2 ]]/[[ VAR3 ]]", var2="there")
        check_injections(result)


def test_check_injections():
    result = inject("hello [[ world ]]", world="there")
    check_injections(result)


def test_check_injections_with_path():
    result = inject(Path("hello [[ world ]]"), world="there")
    check_injections(result)
