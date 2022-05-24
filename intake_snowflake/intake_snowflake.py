from dask_snowflake import read_snowflake

from intake.source import base

from . import __version__


class SnowflakeSource(base.DataSource):
    """
    SQL to dask.dataframe reader.

    Parameters
    ----------
    sql_expr: str
        Query expression to pass to the DB backend
    username: str or None
        Username if not supplied as part of the URI
    password: str or None
        Password if not supplied as part of the URI
    connect_timeout: float or None
        A timeout for the auth connect call, in seconds. None means
        that the timeout defaults to an implementation-specific value.
    request_timeout: float or None
        A timeout for the request call, in seconds. None means that
        the timeout defaults to an implementation-specific value.
    """
    name = 'snowflake'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, sql_expr, username=None, password=None, account=None,
                 arrow_options={}, metadata={}):
        self._init_args = {
            'sql_expr': sql_expr,
            'username': username,
            'password': password,
            'account' : account,
            'arrow_options': arrow_options,
            'metadata': metadata
        }
        self._sql_expr = sql_expr
        self._user = username
        self._password = password
        self._account = account
        self._arrow_options = arrow_options
        self._dataframe = None
        super(SnowflakeSource, self).__init__(metadata=metadata)

    def _load(self):
        self._dataframe = read_snowflake(
            query=self._sql_expr,
            connection_kwargs={
                "user": self._user,
                "password": self._password,
                "account": self._account,
            },
        )

    def _get_schema(self):
        if self._dataframe is None:
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe.dtypes,
                           shape=self._dataframe.shape,
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe.get_partition(i)

    def read(self):
        """Load entire dataset into a container and return it"""
        if self._dataframe is None:
            self._load()
        return self._dataframe.compute()

    def to_dask(self):
        if self._dataframe is None:
            self._load()
        """Return a dask container for this data source"""
        return self._dataframe

    def _close(self):
        self._dataframe = None
