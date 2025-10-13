from dotenv import load_dotenv
import os
from typing import Dict

load_dotenv()


class SnowflakeConfig:
    """Snowflake configuration manager"""

    @staticmethod
    def get_connection_params() -> Dict[str, str]:
        """Get Snowflake connection parameters"""
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE", "DEV_ROLE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "TEST_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "TEST_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "TEST_SCHEMA"),
        }

    @staticmethod
    def get_cortex_model() -> str:
        """Get Cortex model name"""
        return os.getenv("CORTEX_MODEL", "mistral-7b")


# Validate configuration
def validate_config():
    """Validate that all required environment variables are set"""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    print("✓ Configuration validated successfully")
