from ._version import get_versions

__version__ = get_versions()['version']

del get_versions

import intake # noqa

from .intake_snowflake import SnowflakeSource # noqa
from .snowflake_cat import SnowflakeCatalog # noqa
