# Migration 0.19.0

## Run

```bash
go run main.go --dataSource <signoz_db_file_path>
```

## Docker

```bash
docker build -t signoz-migration .
docker run -v <signoz_db_file_path>:/signoz.db signoz-migration --dataSource /signoz.db
```