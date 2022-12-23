# pylint: disable=no-member, no-self-argument, unused-argument
"""Pydantic schema for YAML files"""

import typing

import pydantic

import dynamicio.config.pydantic.io_resources as env_spec
import dynamicio.config.pydantic.table_schema as table_spec


class IOBinding(pydantic.BaseModel):
    """A binding for a single i/o object"""

    name: str = pydantic.Field(alias="__binding_name__")
    environments: typing.Mapping[str, env_spec.IOEnvironment]
    dynamicio_schema: typing.Union[table_spec.DataframeSchema, table_spec.DataframeSchemaRef, None] = pydantic.Field(default=None, alias="schema")

    def get_binding_for_environment(self, environment: str) -> env_spec.IOEnvironment:
        """Fetch the IOEnvironment spec for the name provided."""
        return self.environments[environment]

    @pydantic.validator("environments", pre=True, always=True)
    def pick_correct_env_cls(cls, value, values, config, field):
        """This pre-validator picks an appropriate IOEnvironment subclass for the `data_backend_type`"""
        if not isinstance(value, typing.Mapping):
            raise ValueError(f"Environments input should be a dict. Got {value!r} instead.")
        config_cls_overrides = {
            env_spec.DataBackendType.local: env_spec.LocalDataEnvironment,
            env_spec.DataBackendType.local_batch: env_spec.LocalBatchDataEnvironment,
            env_spec.DataBackendType.s3: env_spec.S3DataEnvironment,
            env_spec.DataBackendType.s3_file: env_spec.S3DataEnvironment,
            env_spec.DataBackendType.s3_path_prefix: env_spec.S3PathPrefixEnvironment,
            env_spec.DataBackendType.kafka: env_spec.KafkaDataEnvironment,
            env_spec.DataBackendType.postgres: env_spec.PostgresDataEnvironment,
        }
        out_dict = {}
        for (env_name, env_data) in value.items():
            base_obj: env_spec.IOEnvironment = field.type_(**env_data)
            override_cls = config_cls_overrides.get(base_obj.data_backend_type)
            if override_cls:
                use_obj = override_cls(**env_data)
            else:
                use_obj = base_obj
            out_dict[env_name] = use_obj
        return out_dict

    @pydantic.root_validator(pre=True)
    def _preprocess_raw_config(cls, values):
        if not isinstance(values, typing.Mapping):
            raise ValueError(f"IOBinding must be a dict at the top level. (got {values!r} instead)")
        remapped_value = {"environments": {}}
        for (key, value) in values.items():
            if key in ("__binding_name__", "schema"):
                # Passthrough params
                remapped_value[key] = value
            else:
                # Assuming an environment config
                remapped_value["environments"][key] = value
        return remapped_value


class BindingsYaml(pydantic.BaseModel):
    """Class controlling structure of the top-level IOConfig yaml file.

    The top-level config is a dictionary of <binding_name> -> <env_name>
    """

    bindings: typing.Mapping[str, IOBinding]

    @pydantic.validator("bindings", pre=True)
    def _validate_bindings(cls, value: typing.Mapping):
        if not isinstance(value, typing.Mapping):
            raise ValueError(f"Bindings must be a mapping. (got {value!r} instead).")
        # Tell each binding its name
        for (name, sub_config) in value.items():
            if not isinstance(sub_config, typing.Mapping):
                raise ValueError(f"Each element for the name binding must be a dict. (got {sub_config!r} instead)")
            sub_config["__binding_name__"] = name
        return value

    def update_config_refs(self, schema_loader) -> "BindingsYaml":
        """Updates dynamic parts of the config:
        - Configure _parent for all `IOEnvironment`s
        - Replace all IOSchemaRef with actual schema objects
        """
        for binding in self.bindings.values():
            if isinstance(binding.dynamicio_schema, table_spec.DataframeSchemaRef):
                binding.dynamicio_schema = schema_loader(binding.dynamicio_schema)
            for io_env in binding.environments.values():
                io_env.set_parent(binding)
        return self
