# Intake-snowflake

Snowflake Plugin for Intake based on dask-snowflake


## User Installation

To install the intake-snowflake plugin, execute the following command

```
conda install -c conda-forge intake-snowflake
```

or:

```
pip install intake-snowflake
```

## Example

An Intake catalog referencing a snowflake dataset consists of the `uri` pointing to the snowflake instance along with a username and password and a SQL expression (`sql_expr`), e.g.:

```yaml
sources:
  dremio_vds:
    driver: dremio 
    args:
        username: <SNOWFLAKE_USER>
        password: <SNOWFLAKE_PASSWORD>
        account: <SNOWFLAKE_ACCOUNT>
        sql_expr: SELECT * FROM TABLE ORDER BY "timestamp" ASC
```
