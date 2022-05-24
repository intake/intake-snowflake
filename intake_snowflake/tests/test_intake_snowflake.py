import intake

from intake_snowflake import SnowflakeSource

# pytest imports this package last, so plugin is not auto-added
intake.registry['snowflake'] = DremioSource
