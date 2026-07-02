FROM apache/airflow:2.10.4

COPY --chown=airflow:0 requirements.txt /opt/airflow/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy fallback directories (baked files)
COPY --chown=airflow:0 dags/ /opt/airflow/dags_baked/
COPY --chown=airflow:0 src/ /opt/airflow/src_baked/
COPY --chown=airflow:0 config/ /opt/airflow/config_baked/

# Copy the custom entrypoint script
COPY docker/airflow/entrypoint.sh /opt/airflow/entrypoint.sh

USER root
RUN chmod +x /opt/airflow/entrypoint.sh

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/entrypoint.sh"]
