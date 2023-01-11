# pylint: disable=no-member, no-self-argument, unused-argument
"""Pydantic schema for YAML files"""

from typing import Mapping, MutableMapping

import pydantic

import dynamicio.config.pydantic.io_resources as env_spec


class BindingsYaml(pydantic.BaseModel):
    """Class controlling structure of the top-level IOConfig yaml file.

    The top-level config is a dictionary of <binding_name> -> <env_name>
    """

    bindings: Mapping[str, env_spec.IOBinding]

    @pydantic.validator("bindings", pre=True)
    def _validate_bindings(cls, value: Mapping):
        if not isinstance(value, Mapping):
            raise ValueError(f"Bindings must be a mapping. (got {value!r} instead).")
        # Tell each binding its name
        for (name, sub_config) in value.items():
            if not isinstance(sub_config, MutableMapping):
                raise ValueError(f"Each element for the name binding must be a dict. (got {sub_config!r} instead)")
            sub_config["__binding_name__"] = name
        return value

    def update_config_refs(self) -> "BindingsYaml":
        """Updates dynamic parts of the config:
        - Configure _parent for all `IOEnvironment`s
        - Replace all IOSchemaRef with actual schema objects
        """
        for binding in self.bindings.values():
            for io_env in binding.environments.values():
                io_env.set_parent(binding)
        return self
