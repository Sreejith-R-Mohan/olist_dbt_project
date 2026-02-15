FROM python:3.14-slim


WORKDIR /app


COPY olist_dbt /app

RUN pip install dbt-snowflake


CMD ["dbt","build"]


