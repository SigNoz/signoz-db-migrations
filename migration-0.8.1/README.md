# Migrate Dashboard

This application can be used to migrate existing dashboards.

It helps sanitizes previous dashboard data:
- proper widget id included in layout
- adds relation between layout and widgets

## Running Migration Script

### Binary

Clone this repository and run the following command:

```bash
go build

./migrate-dashboard --dataSource <path-to-signoz.db>
```

### Docker Standalone

`cd` to SigNoz repository and run following commands:

```bash
cd deploy/docker/clickhouse-setup

docker run -it -v $PWD/data/signoz/:/var/lib/signoz/ signoz/migrate-dashboard:0.8.1
```

### Docker Swarm

`cd` to SigNoz repository and run following commands:

```bash
cd deploy/swarm/clickhouse-setup

docker run -it -v $PWD/data/signoz/:/var/lib/signoz/ signoz/migrate-dashboard:0.8.1
```

Note: In case of multi node swarm cluster, run the above commands in the node where query-service is running: `docker service ps query-service`.

### Kubernetes

To download `migrate-dashboard` binary:

```bash
wget https://github.com/signoz/migrate-dashboard/releases/download/v0.8.1/migrate-dashboard-v0.8.1-linux-amd64.tar.gz

tar xzvf migrate-dashboard-v0.8.1-linux-amd64.tar.gz
```

To copy the binary in persistent volume path `/var/lib/signoz` in `query-service`:

```bash
kubectl cp -n platform ./migrate-dashboard my-release-signoz-query-service-0:/var/lib/signoz/migrate-dashboard
```

To `exec` into the `query-service` container:

```bash
kubectl -n platform exec -it pod/my-release-signoz-query-service-0 -- sh
```

Now, change directory to the `/var/lib/signoz` and run the migration script:

```bash
cd /var/lib/signoz

./migrate-dashboard
```

### CLI Flags

There are is only one flag in the `migrate-dashboard` binary:

Flags:

- `--dataSource` : Data Source path. `default=/var/lib/signoz/signoz.db`