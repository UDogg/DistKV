# DistKV historian: records cluster history into SQLite.
FROM python:3.13-alpine
WORKDIR /app
COPY distkv ./distkv
RUN adduser -D -u 10001 historian && mkdir /data && chown historian /data
USER historian
VOLUME ["/data"]
EXPOSE 8090
ENTRYPOINT ["python", "-m", "distkv.historian", "--listen", "0.0.0.0:8090", "--db", "/data/history.sqlite3"]
