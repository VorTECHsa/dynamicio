"""Default dynamicio mixins module"""

from dynamicio.mixins.with_kafka import (
    WithKafka,
)
from dynamicio.mixins.with_local import (
    WithLocal,
    WithLocalBatch,
)
from dynamicio.mixins.with_postgres import (
    WithPostgres,
)
from dynamicio.mixins.with_s3 import (
    WithS3File,
    WithS3PathPrefix,
)
