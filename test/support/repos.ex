defmodule Outboxer.Test.PgRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :outboxer, adapter: Ecto.Adapters.Postgres
end

defmodule Outboxer.Test.SqliteRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :outboxer, adapter: Ecto.Adapters.SQLite3
end

defmodule Outboxer.Test.Migration do
  @moduledoc false
  use Ecto.Migration

  def up, do: Outboxer.Migration.up()
  def down, do: Outboxer.Migration.down()
end

defmodule Outboxer.Test.ObanMigration do
  @moduledoc false
  use Ecto.Migration

  def up, do: Oban.Migration.up()
  def down, do: Oban.Migration.down(version: 1)
end

defmodule Outboxer.Test.PrefixedMigration do
  @moduledoc false
  use Ecto.Migration

  def up, do: Outboxer.Migration.up(prefix: "private", table: "custom_outbox")
  def down, do: Outboxer.Migration.down(prefix: "private", table: "custom_outbox")
end
