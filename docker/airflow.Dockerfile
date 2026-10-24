# Airflow 2.10 with the pipeline's own virtual environment beside it. Airflow keeps its pinned
# dependencies; the data libraries (pyarrow, pandas, Great Expectations, dbt) live in /opt/pipeline-venv,
# and tasks call `taxi-platform ...` / `dbt ...` from there as subprocesses.
FROM apache/airflow:2.10.5-python3.12

USER root
RUN python -m venv /opt/pipeline-venv && chown -R airflow:0 /opt/pipeline-venv && mkdir -p /opt/pipeline /opt/airflow/alert-sink \
    && chown -R airflow:0 /opt/pipeline /opt/airflow/alert-sink

USER airflow
COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:0 pyproject.toml /opt/pipeline/pyproject.toml
COPY --chown=airflow:0 taxi_platform /opt/pipeline/taxi_platform
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir --no-deps -e /opt/pipeline

# Airflow's own interpreter can import taxi_platform.alerts (standard library only) for its callbacks.
ENV PYTHONPATH=/opt/pipeline \
    DBT_TARGET_PATH=/tmp/dbt-target \
    DBT_LOG_PATH=/tmp/dbt-logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=False
