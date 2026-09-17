# pingstats
Easy ping, jitter and loss monitoring tool for Windows.

![Screenshot](/screenshots/screen0.png?raw=true)

## Logging and history

`pingstats.cfg` is written on every start, so the keys below show up with
their defaults after the first run.

```
db {
	enabled = false;
	path = pingstats.db;
	retentionDays = 7;
	restoreMinutes = 60;
	postgresUri = ;
	postgresTable = pingstats;
	postgresSyncSeconds = 60;
	postgresRetentionDays = 30;
}
```

| key | meaning |
| --- | --- |
| `enabled` | write every ping result to SQLite |
| `path` | database file, WAL mode, one table per day |
| `retentionDays` | whole days older than this are dropped from SQLite |
| `restoreMinutes` | how much history is reloaded into the graph on start, `0` to disable |
| `postgresUri` | libpq connection URI, empty for no PostgreSQL sync |
| `postgresTable` | the single table results are synced into |
| `postgresSyncSeconds` | how often queued results are sent |
| `postgresRetentionDays` | how long PostgreSQL keeps results, `0` keeps everything |

The config format has no comment syntax - anything that is not a
`key = value;` pair or a `name { ... }` block will be parsed as part of the
next key.

With `enabled = true`, pingstats reloads the last `restoreMinutes` of results
for each host when it starts, so the graphs come back instead of starting
empty. Whatever falls outside that window - or outside the time the plot can
show - is simply not drawn, and a stretch where pingstats wasn't running
leaves a gap in the line rather than a straight line across it.

`postgresUri` is a [libpq connection
URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS),
for example `postgresql://user:password@host:5432/pingstats?sslmode=require`.
When it is set, results go to PostgreSQL as well as to SQLite. A background
thread sends them once every `postgresSyncSeconds`, and keeps them queued and
retries while the server is unreachable. SQLite remains the local source of
truth and the one the graphs are restored from.

On connect, pingstats looks `postgresTable` up and creates it if it isn't
there:

```sql
CREATE TABLE pingstats (
	ts TIMESTAMPTZ NOT NULL,
	host TEXT NOT NULL,
	section TEXT NOT NULL,
	latency_ms DOUBLE PRECISION,   -- NULL when the ping was lost
	error_code INTEGER NOT NULL,
	status_code INTEGER NOT NULL,
	responder TEXT,
	sys_latency_ms INTEGER) PARTITION BY RANGE (ts);
```

Everything lands in that one table. It is range partitioned by ISO week in
UTC, and the weekly partition - `pingstats_2026w38` and so on - is created
the first time a result of that week is inserted. Querying and inserting
still go through the parent table, so the partitioning only shows up when you
want it to: dropping a week is one `DROP TABLE`, which is what `postgresRetentionDays`
does for you. Because a partition can only go once its newest result has aged
out, results live at least `postgresRetentionDays` and at most a week longer
than that. `0` keeps everything. Rows carry the machine's hostname, so several
machines can share the table.

The two retentions are deliberately separate: SQLite is the local buffer the
graphs are restored from and keeps a week, while PostgreSQL is the archive and
keeps a month.

If `postgresTable` already exists as a plain, non-partitioned table,
pingstats just inserts into it and leaves its structure and contents alone.
