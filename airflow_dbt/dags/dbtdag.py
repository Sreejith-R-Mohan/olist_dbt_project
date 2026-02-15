import os
from datetime import datetime

from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig,ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping






profile = SnowflakeUserPasswordProfileMapping(
    conn_id = 'snowflake_conn',
    profile_args = {"database":"dbt_db","schema":"analytics"}
)


profile_config = ProfileConfig(
    profile_name ="default",
    target_name="dev",
    profile_mapping= profile
)


dbt_snowflake_dag = DbtDag(
    project_config = ProjectConfig("/usr/local/airflow/include/olist_dbt",),
    profile_config=profile_config,
    execution_config = ExecutionConfig(dbt_executable_path=f'{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt'),
    schedule = "@daily", 
    start_date = datetime(2026,2,15),
    catchup=False,
    dag_id="my_dag"
)
