import pandas as pd

from demo.src.io import Foo


def test_pandera_validations():

    # Given
    df = pd.read_csv("/Users/chadjinik/Github/Vortexa/dynamicio/demo/tests/data/input/foo.csv")

    # When
    Foo.validate(df)

    # Then