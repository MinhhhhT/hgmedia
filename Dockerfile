FROM apache/airflow:3.2.1-python3.12

USER root

# The ETL code connects to SQL Server through ODBC Driver 17.
# socat is used only in LOCAL_SOURCE_PROXY_MODE to expose unroutable fake
# source-system IP/port pairs inside each runtime container.
# The upstream Airflow image may already configure packages.microsoft.com.
# Register the Microsoft APT repository only when it is not already present;
# re-registering it can cause conflicting Signed-By values.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl gnupg postgresql-client unixodbc unixodbc-dev socat \
    && if ! grep -Rqs "packages.microsoft.com/debian/12/prod" \
         /etc/apt/sources.list /etc/apt/sources.list.d 2>/dev/null; then \
         curl -fsSLo /tmp/packages-microsoft-prod.deb \
           https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb; \
         dpkg -i /tmp/packages-microsoft-prod.deb; \
         rm -f /tmp/packages-microsoft-prod.deb; \
       fi \
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
