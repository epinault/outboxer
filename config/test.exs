import Config

config :logger, level: :warning

config :outboxer, ecto_repos: [Outboxer.Test.PgRepo, Outboxer.Test.SqliteRepo]

config :outboxer, Outboxer.Test.PgRepo,
  username: "postgres",
  password: "postgres",
  hostname: System.get_env("OUTBOXER_PG_HOST", "localhost"),
  port: String.to_integer(System.get_env("OUTBOXER_PG_PORT", "54329")),
  database: "outboxer_test",
  pool_size: 20,
  log: false

config :outboxer, Outboxer.Test.SqliteRepo,
  database: Path.expand("../_build/test/outboxer_test.sqlite3", __DIR__),
  pool_size: 5,
  busy_timeout: 10_000,
  log: false

# Oban's own test instance (see test/outboxer/relay/oban_test.exs)
config :outboxer, Oban,
  repo: Outboxer.Test.PgRepo,
  testing: :manual,
  queues: false,
  plugins: false

# ex_aws, only used by the SQS integration test against MiniStack
config :ex_aws,
  http_client: ExAws.Request.Req,
  access_key_id: "test",
  secret_access_key: "test",
  region: "us-east-1"
