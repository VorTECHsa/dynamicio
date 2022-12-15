"""Default dynamicio mixins module"""

from .with_kafka import (
    WithKafka,
)
from .with_local import (
    WithLocal,
    WithLocalBatch,
)
from .with_postgres import (
    WithPostgres,
)
from .with_s3 import (
    WithS3File,
    WithS3PathPrefix,
)
