import uuid
import pkg_resources

from functools import lru_cache

from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__
from .intake_snowflake import SnowflakeSource

STATIC_FILES = ("static/style.css",)

@lru_cache(None)
def _load_static_files():
    """Lazily load the resource files into memory the first time they are needed"""
    return [
        pkg_resources.resource_string("intake_snowflake", fname).decode("utf8")
        for fname in STATIC_FILES
    ]


class SnowflakeCatalog(Catalog):

    name = 'snowflake_cat'

    __version__ = __version__

    _sql_expr = 'select * from {database}.INFORMATION_SCHEMA."TABLES"'

    def __init__(self, database='SNOWFLAKE', username=None, password=None, account=None, **kwargs):
        self._source = SnowflakeSource(
            self._sql_expr.format(database=database), username=username,
            password=password, account=account
        )
        self._database = database
        self._dataframe = None
        super(SnowflakeCatalog, self).__init__(**kwargs)

    def _load(self):
        self._dataframe = self._source.read()
        for _, row in self._dataframe.iterrows():
            self._create_entry(row)

    def __getitem__(self, key):
        normalized_key = key.replace('"', '').lower()
        cats = list(self)
        normalized_cats = [cat.replace('"', '').lower() for cat in cats]
        if key not in cats and normalized_key in normalized_cats:
            key = cats[normalized_cats.index(normalized_key)]
        return super().__getitem__(key)

    def _create_entry(self, row):
        path = f'{self._database}."{row.TABLE_SCHEMA}"."{row.TABLE_NAME}"'
        description = f'Snowflake {row.TABLE_TYPE} {path}'
        args = dict(self._source._init_args, sql_expr=f'SELECT * FROM {path}')
        e = LocalCatalogEntry(path, description, 'dremio', True,
                              args, {}, {}, {}, "", getenv=True,
                              getshell=False)
        e._plugin = [SnowflakeSource]
        self._entries[path] = e

    # Override _ipython_display_ from baseclass
    _ipython_display_ = None

    def _repr_html_(self):
        (css_style,) = _load_static_files()
        uid = str(uuid.uuid4())
        info = self._source._init_args
        db_prefix = self._database+'.'
        entries = []
        for e in self:
            euid = str(uuid.uuid4())
            erepr = repr(self[e])
            e = e.replace(db_prefix, '')
            entry = f"""
            <li class="dr-entry-item">
              <div class="dr-entry-name"><span>{e}</span></div>
              <input id="entry-{euid}" class="dr-entry-data-in" type="checkbox"></input>
              <label for="entry-{euid}" class="dr-entry-data-label"><b>+</b></label>
              <div class="dr-entry-data">
                <pre>{erepr}</pre>
                <br>
              </div>
            </li>
            """
            entries.append(entry)
        entry_html = '\n'.join(entries)
        return f"""
        <div class="dr-wrap">
        <style>{css_style}</style>
        <div class="dr-header">
          <div class="dr-obj-type">SnowflakeCatalog: {self._database} on {info['account']}</div>
        </div>
        <ul class="dr-sections">
          <li class="dr-section-item">
            <input id="section-{uid}" class="dr-section-summary-in" type="checkbox"></input>
            <label for="section-{uid}" class="dr-section-summary">Entries</label>
            <div class="dr-section-inline-details">{len(self)}</div>
            <div class="dr-section-details">
              <ul class="dr-entry-list">
              {entry_html}
              </ul>
            </div>
          </li>
        </ul>
        </div>
        """

    def create_source(self, sql_expr):
        """
        Create a SnowflakeSource from the provided sql_expr with the 
        credentials declared on this catalog.

        Parameters
        ----------
        sql_expr: str
            The SQL query to declare on the source.

        Returns
        -------
        SnowflakeSource:
            Returns the SnowflakeSource containing the provided sql_expr
        """
        args = dict(self._source._init_args, sql_expr=sql_expr)
        return SnowflakeSource(**args)
