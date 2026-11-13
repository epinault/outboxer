# The suite runs every database test against SQLite and, when it is reachable,
# against Postgres (`docker compose up -d postgres`).
#
#   mix test                     # SQLite + Postgres, relays against fake clients
#   mix test --exclude postgres  # SQLite only, no Docker needed
#   mix test.integration         # also real Kafka and SQS (docker compose up -d)

alias Outboxer.Test.Migration
alias Outboxer.Test.PgRepo
alias Outboxer.Test.SqliteRepo

excluded = ExUnit.configuration()[:exclude]
sqlite_file = SqliteRepo.config()[:database]
for suffix <- ["", "-wal", "-shm"], do: File.rm(sqlite_file <> suffix)

# Create and migrate the file through a single connection first: several
# connections racing to create a fresh SQLite file report "database is locked".
{:ok, bootstrap} = SqliteRepo.start_link(pool_size: 1)
Ecto.Migrator.up(SqliteRepo, 1, Migration, log: false)
Supervisor.stop(bootstrap)
{:ok, _pid} = SqliteRepo.start_link()

postgres? =
  if :postgres in excluded do
    false
  else
    Process.flag(:trap_exit, true)

    with {:ok, _pid} <- PgRepo.start_link(),
         {:ok, _result} <- PgRepo.query("SELECT 1", [], log: false) do
      # Start from a clean slate, including Oban's tables.
      PgRepo.query!("DROP SCHEMA public CASCADE")
      PgRepo.query!("CREATE SCHEMA public")
      Ecto.Migrator.up(PgRepo, 1, Migration, log: false)
      Ecto.Migrator.up(PgRepo, 2, Outboxer.Test.ObanMigration, log: false)
      true
    else
      _unavailable -> false
    end
  end

if not postgres? and :postgres not in excluded do
  Mix.shell().info("""

  !! Postgres is not reachable, so Postgres tests are skipped.
     Start it with `docker compose up -d postgres`, or silence this with `mix test --exclude postgres`.
  """)
end

repos = if postgres?, do: [SqliteRepo, PgRepo], else: [SqliteRepo]
Application.put_env(:outboxer, :test_repos, repos)

exclude = [:integration] ++ if(postgres?, do: [], else: [:postgres])
ExUnit.start(exclude: exclude)
