"""Runs a test in an explicitly chosen environment of the resource definitions."""

from typing import Dict, Optional

from dynamicio.config import IOConfig


class EnvironmentSwitch:
    """Context manager that makes configs select ``environment`` and restores their own on exit.

    A task picks the resource definitions of its config's environment, so a test that needs a variant (``LOCAL_REMOVED_UPDATED``, ``LOCAL_E2E`` ...) must
    switch the configs the task uses. :class:`ExpectedOutputs` does this for every ``IOConfig`` that exists, so you rarely need this class; use it to switch
    only some configs, for example when you do not check outputs::

        with EnvironmentSwitch("LOCAL_E2E", input_config, output_config):
            Task().run()

    Args:
        environment: The environment name, as it appears in the resource definitions. ``None`` selects the default environment of each config (the
            first one its resource definitions list).
        configs: The ``IOConfig`` objects to switch.

    Raises:
        ValueError: On entering, if a config defines no resource in ``environment`` (usually a typo).
    """

    def __init__(self, environment: Optional[str], *configs: IOConfig):
        self.environment = environment
        self.configs = configs
        self.skip_configs_without_environment = False
        self._original: Dict[int, str] = {}
        self._switched: Dict[int, IOConfig] = {}

    @classmethod
    def of_all(cls, environment: Optional[str]) -> "EnvironmentSwitch":
        """A switch of every ``IOConfig`` that is alive; those that do not define ``environment`` are left alone, unless none does."""
        switch = cls(environment, *IOConfig.instances())
        switch.skip_configs_without_environment = True
        return switch

    @staticmethod
    def available(config: IOConfig) -> set:
        """The environment names the resource definitions of ``config`` define."""
        return {name for binding in config.config.bindings.values() for name in binding.environments}

    def __enter__(self) -> "EnvironmentSwitch":
        targets = {}
        for config in self.configs:
            target = self.environment or config.default_environment
            if target in self.available(config):
                targets[id(config)] = (config, target)
            elif not self.skip_configs_without_environment:
                raise ValueError(f"{config.path_to_source_yaml} defines no resource in environment {target!r}; it defines {sorted(self.available(config))}")
        if self.configs and not targets:
            raise ValueError(f"no resource definitions define the environment {self.environment!r}")
        for key, (config, target) in targets.items():
            self._original[key] = config.env_identifier
            config.env_identifier = target
        self._switched = {key: config for key, (config, _) in targets.items()}
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        for key, config in self._switched.items():
            config.env_identifier = self._original.pop(key)
        self._switched = {}
