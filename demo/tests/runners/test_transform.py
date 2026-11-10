# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
# noqa
import asyncio

import pytest

from demo.src import transform_output_config
from demo.src.runners import transform
from dynamicio.testharness import ExpectedOutputs


class TestTransform:
    @pytest.mark.integration
    def test_transform_writes_the_final_foo_and_bar_datasets(self, staged_inputs):
        # Given the staged datasets in place (see the `staged_inputs` fixture)

        # When
        with ExpectedOutputs() as outputs:
            asyncio.run(transform.main())

            # Then
            outputs.check(transform_output_config, "FINAL_FOO")
            outputs.check(transform_output_config, "FINAL_BAR")
