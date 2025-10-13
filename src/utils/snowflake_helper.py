from typing import Optional, Dict

import pandas as pd
from snowflake.snowpark import Session


class SnowflakeHelper:
    """Helper class for Snowflake operations"""

    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.session: Optional[Session] = None

    def connect(self) -> Session:
        """Create and return Snowflake session"""
        if self.session is None:
            self.session = Session.builder.configs(self.connection_params).create()
            print(f"✓ Connected to Snowflake as {self.connection_params['user']}")
            print(f"  Role: {self.session.get_current_role()}")
            print(f"  Warehouse: {self.session.get_current_warehouse()}")
            print(f"  Database: {self.session.get_current_database()}")
            print(f"  Schema: {self.session.get_current_schema()}")
        return self.session

    def disconnect(self):
        """Close Snowflake session"""
        if self.session:
            self.session.close()
            self.session = None
            print("✓ Disconnected from Snowflake")

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame"""
        if not self.session:
            self.connect()
        return self.session.sql(query).to_pandas()

    def cortex_complete(self, prompt: str, model: str = "mistral-7b") -> str:
        """Use Cortex Complete for text generation"""
        if not self.session:
            self.connect()

        from snowflake.cortex import complete
        result = complete(model, prompt, session=self.session)
        return result

    def cortex_search(self, service_name: str, query: str, columns: list, limit: int = 5) -> pd.DataFrame:
        """Use Cortex Search for semantic search"""
        if not self.session:
            self.connect()

        # Build search query
        columns_str = ", ".join(columns)
        search_query = f"""
        SELECT {columns_str}
        FROM TABLE(
            {service_name}.SEARCH(
                '{query}',
                {limit}
            )
        )
        """
        return self.execute_query(search_query)

    def load_data_to_table(self, df: pd.DataFrame, table_name: str, overwrite: bool = False):
        """Load pandas DataFrame to Snowflake table"""
        if not self.session:
            self.connect()

        mode = "overwrite" if overwrite else "append"
        snowpark_df = self.session.create_dataframe(df)
        snowpark_df.write.mode(mode).save_as_table(table_name)
        print(f"✓ Loaded {len(df)} rows to {table_name}")
