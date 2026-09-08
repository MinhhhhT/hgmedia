FROM apache/airflow:3.2.1-python3.12

USER root

# The ETL code connects to SQL Server through ODBC Driver 17.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg postgresql-client unixodbc unixodbc-dev \
    && curl -fsSLo /tmp/packages-microsoft-prod.deb \
       https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
    && dpkg -i /tmp/packages-microsoft-prod.deb \
    && rm /tmp/packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements-docker.txt /tmp/requirements-docker.txt
COPY --chmod=755 --chown=airflow:root scripts/docker-entrypoint.sh /opt/airflow/docker-entrypoint.sh

RUN pip install --no-cache-dir -r /tmp/requirements-docker.txt \
    && pip check

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/docker-entrypoint.sh"]
