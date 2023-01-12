"""Test regressions discovered in v4.3.0 release"""

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.core import SCHEMA_FROM_FILE


class IO(UnifiedIO):
    schema = SCHEMA_FROM_FILE


def test_missing_validations_and_metrics(regressions_resources_dir, regressions_constants_module):
    """Dynamicio was refusing to work with schemas that did not have any validations specified."""
    # Given
    input_config = IOConfig(
        path_to_source_yaml=regressions_resources_dir / "missing_v430_validations.yaml",
        env_identifier="LOCAL",
        dynamic_vars=regressions_constants_module,
    )
    io_instance = IO(source_config=input_config.get(source_key="PRODUCTS"), apply_schema_validations=True, log_schema_metrics=True)

    # When
    data = io_instance.read()

    # Then
    assert data.to_dict() == {"id": {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9, 9: 10, 10: 11, 11: 12, 12: 13, 13: 14, 14: 15}}
