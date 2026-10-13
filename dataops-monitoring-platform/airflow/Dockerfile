# Airflow + Java + PySpark in one image, so tasks can run Spark in local mode.
FROM apache/airflow:2.10.5-python3.11

USER root
# mkdir is needed because slim Debian images lack the man folder Java's installer expects.
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
# Pinning apache-airflow stops pip from upgrading/downgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /requirements.txt
