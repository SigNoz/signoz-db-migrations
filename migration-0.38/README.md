
This connects to both clickhouse and sqlite

* Run the image
```
docker run --name signoz-migrate-sqlite --network clickhouse-setup_default -it \
-v $PWD/data/signoz/:/var/lib/signoz/ signoz/migrate:0.38 \
-data_source=/var/lib/signoz/signoz.db \
-host=host.docker.internal \
-port=9000
```


## Local testing
* Build the image
```
docker build -t signoz/migrate:0.38 .
```

* Run the container
```
docker run --rm --name signoz-migrate-sqlite --network clickhouse-setup_default -it \
-v ./db:/var/lib/signoz/signoz.db signoz/migrate:0.38 \
--data_source=/var/lib/signoz/signoz.db \
--host=host.docker.internal \
--port=9000
```