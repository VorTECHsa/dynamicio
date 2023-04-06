# flake8: noqa: I101

from datetime import datetime

import numpy as np
import pytest

from dynamicio.inject import (
    InjectionError,
    _check_curly_braces_injections,
    _check_square_bracket_injections,
    _inject_curly_braces_vars,
    _inject_square_bracket_vars,
)


def test_inject_square_bracket_vars():
    res = _inject_square_bracket_vars("hello [[ world ]]", world="there")
    assert res == "hello there"


def test_inject_square_bracket_vars_kwargs_is_case_insensitive():
    res = _inject_square_bracket_vars("hello [[ world ]]", WORLD="there")
    assert res == "hello there"


def test_inject_square_bracket_vars_value_is_case_insensitive():
    res = _inject_square_bracket_vars("hello [[ WOrLD ]]", world="there")
    assert res == "hello there"


def test_inject_square_bracket_vars_matches_multiple():
    res = _inject_square_bracket_vars("[[ VAR1 ]]/[[VAR2]]", var1="hello", var2="there")
    assert res == "hello/there"


def test_inject_square_bracket_vars_various_data_types():
    res = _inject_square_bracket_vars(
        "[[ VAR1 ]]/[[ VAR2 ]]/[[ VAR3 ]]/[[ VAR4 ]]/[[ VAR5 ]]/[[ VAR6 ]]",
        var1=1,
        var2=[1, 2, 3],
        var3={"hello": "there"},
        var4=34.98,
        var5=datetime(2021, 1, 1),
        var6=np.array([1, 2, 3]),
    )
    assert res == "1/[1, 2, 3]/{'hello': 'there'}/34.98/2021-01-01 00:00:00/[1 2 3]"


def test_inject_square_bracket_vars_accepts_extra():
    res = _inject_square_bracket_vars("[[ VAR1 ]]", var1="hello", var2="there", var3="extra")
    assert res == "hello"


def test_inject_curly_braces_vars():
    res = _inject_curly_braces_vars("hello {world}", world="there")
    assert res == "hello there"


def test_inject_curly_braces_vars_kwargs_is_case_insensitive():
    res = _inject_curly_braces_vars("hello {world}", WORLD="there")
    assert res == "hello there"


def test_inject_curly_braces_vars_value_is_case_insensitive():
    res = _inject_curly_braces_vars("hello {WOrLD}", world="there")
    assert res == "hello there"


def test_inject_curly_braces_vars_matches_multiple():
    res = _inject_curly_braces_vars("{VAR1}/{VAR2}", var1="hello", var2="there")
    assert res == "hello/there"


def test_inject_curly_braces_vars_various_data_types():
    res = _inject_curly_braces_vars(
        "{VAR1}/{VAR2}/{VAR3}/{VAR4}/{VAR5}/{VAR6}",
        var1=1,
        var2=[1, 2, 3],
        var3={"hello": "there"},
        var4=34.98,
        var5=datetime(2021, 1, 1),
        var6=np.array([1, 2, 3]),
    )
    assert res == "1/[1, 2, 3]/{'hello': 'there'}/34.98/2021-01-01 00:00:00/[1 2 3]"


def test_inject_curly_braces_vars_accepts_extra():
    res = _inject_curly_braces_vars("{VAR1}", var1="hello", var2="there", var3="extra")
    assert res == "hello"


def test_inject_curly_braces_accepts_no_vars_in_value():
    res = _inject_curly_braces_vars("hi", var1="hello")
    assert res == "hi"


def test_inject_square_bracket_vars_works_correctly_with_multiple_some_not_injected():
    result = _inject_square_bracket_vars("[[ VAR1 ]]/[[ VAR2 ]]/[[ VAR3 ]]", var2="there")
    assert result == "[[ VAR1 ]]/there/[[ VAR3 ]]"


def test_inject_curly_braces_vars_works_correctly_with_multiple_some_not_injected():
    result = _inject_curly_braces_vars("{VAR1}/{VAR2}/{VAR3}", var2="there")
    assert result == "{VAR1}/there/{VAR3}"


def test__check_square_bracket_injections_throws_on_missing_var():
    with pytest.raises(InjectionError):
        result = _inject_square_bracket_vars("[[ VAR1 ]]/[[ VAR2 ]]/[[ VAR3 ]]", var2="there")
        _check_square_bracket_injections(result)


def test_inject_curly_braces_vars_throws_on_missing_var():
    with pytest.raises(InjectionError):
        result = _inject_curly_braces_vars("{VAR1}", var2="there")
        _check_curly_braces_injections(result)
