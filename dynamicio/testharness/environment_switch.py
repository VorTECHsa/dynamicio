"""Runs a test in an explicitly chosen environment of the resource definitions."""

from typing import Dict

from dynamicio.config import IOConfig


class EnvironmentSwitch:
    """Context manager that makes every config in ``configs`` select ``environment`` and restores their own on exit.

    A task picks the resource definitions of its config's environment, so a test that needs a variant (``LOCAL_REMOVED_UPDATED``, ``LOCAL_E2E`` ...) must
    switch the task's input and output configs together, and tell :class:`ExpectedOutputs` which one to check. Use the same name for both, for example
    with pytest's ``parametrize``, instead of patching ``env_identifier`` by hand::

        with EnvironmentSwitch(environment, input_config, output_config), ExpectedOutputs(environment=environment) as outputs:
            Task().run()
            outputs.check(output_config, "FINAL_BAR")

    Args:
        environment: The environment name, as it appears in the resource definitions.
        configs: The ``IOConfig`` objects of the task under test.

    Raises:
        ValueError: On entering, if a config defines no resource in ``environment`` (usually a typo).
    """

    def __init__(self, environment: str, *configs: IOConfig):
        self.environment = environment
        self.configs = configs
        self._original: Dict[int, str] = {}

    def __enter__(self) -> "EnvironmentSwitch":
        for config in self.configs:
            available = sorted({name for binding in config.config.bindings.values() for name in binding.environments})
            if self.environment not in available:
                raise ValueError(f"{config.path_to_source_yaml} defines no resource in environment {self.environment!r}; it defines {available}")
        for config in self.configs:
            self._original[id(config)] = config.env_identifier
            config.env_identifier = self.environment
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        for config in self.configs:
            config.env_identifier = self._original.pop(id(config), config.env_identifier)
