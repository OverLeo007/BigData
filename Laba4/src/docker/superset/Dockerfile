FROM apache/superset:4.1.1

USER root

RUN pip install --no-cache-dir psycopg2-binary

COPY superset_config.py /app/pythonpath/superset_config.py

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset