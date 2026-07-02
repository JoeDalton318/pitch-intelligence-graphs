#!/bin/bash
set -e

# Function to copy files if directory is empty
sync_directory() {
    local src="$1"
    local dest="$2"
    if [ -d "$src" ] && [ -d "$dest" ]; then
        if [ -z "$(ls -A "$dest")" ]; then
            echo "Directory $dest is empty (mounting issue?). Copying fallback files from $src..."
            cp -r "$src"/. "$dest"/
        else
            echo "Directory $dest is not empty. Using mounted files."
        fi
    fi
}

# Change ownership of the directories to airflow so it can read/write them
chown -R airflow:root /opt/airflow/dags /opt/airflow/src /opt/airflow/config

# Sync directories if they are empty (tmpfs fallback)
sync_directory "/opt/airflow/dags_baked" "/opt/airflow/dags"
sync_directory "/opt/airflow/src_baked" "/opt/airflow/src"
sync_directory "/opt/airflow/config_baked" "/opt/airflow/config"

# Ensure airflow user owns the files after copying
chown -R airflow:root /opt/airflow/dags /opt/airflow/src /opt/airflow/config

# Run the official entrypoint as user airflow
exec runuser -u airflow -- /entrypoint "$@"
