defmodule Outboxer.Migration do
  @moduledoc """
  Creates and drops the outbox table.

  Generate a migration in your application and delegate to this module:

      defmodule MyApp.Repo.Migrations.AddOutboxer do
        use Ecto.Migration

        def up, do: Outboxer.Migration.up()
        def down, do: Outboxer.Migration.down()
      end

  The same migration works on Postgres and SQLite.

  ## Options

    * `:table` - table name, default `"outboxer_messages"`. Pass the same value
      as `:table` to `Outboxer`.
    * `:prefix` - Postgres schema to create the table in.
    * `:version` - schema version to migrate to (`up/1`) or back to (`down/1`).
      Defaults to the latest for `up/1` and to `0` (nothing left) for `down/1`.
      Later releases that change the table will add versions; you then write a
      new migration calling `up(version: n)`.

  ## What gets created

  One table and three partial indexes. The indexes that the workers use cover
  only rows with `status = 'pending'`, so their size follows the backlog, not
  the history.
  """

  use Ecto.Migration

  @latest 1

  @doc "Migrates the outbox table up to `:version` (default: latest)."
  def up(opts \\ []) do
    target = Keyword.get(opts, :version, @latest)
    for version <- 1..target//1, do: change(version, :up, opts)
    :ok
  end

  @doc "Migrates the outbox table down to `:version` (default: 0, dropping it)."
  def down(opts \\ []) do
    target = Keyword.get(opts, :version, 0)
    for version <- @latest..(target + 1)//-1, do: change(version, :down, opts)
    :ok
  end

  defp change(1, :up, opts) do
    table = table_name(opts)
    prefix = Keyword.get(opts, :prefix)

    create table(table, prefix: prefix) do
      add(:outbox, :string, null: false)
      add(:partition, :integer, null: false, default: 0)
      add(:group_key, :string)
      add(:type, :string, null: false)
      add(:payload, :binary, null: false)
      add(:headers, :text)
      add(:status, :string, null: false, default: "pending")
      add(:attempts, :integer, null: false, default: 0)
      add(:last_error, :text)
      add(:scheduled_at, :utc_datetime_usec, null: false)
      add(:inserted_at, :utc_datetime_usec, null: false)
      add(:delivered_at, :utc_datetime_usec)
    end

    # The fetch path: ready messages of one partition, oldest first.
    create(
      index(table, [:outbox, :partition, :id],
        prefix: prefix,
        where: "status = 'pending'",
        name: "#{table}_pending_index"
      )
    )

    # Finding blocked groups: only rows that have failed at least once.
    create(
      index(table, [:outbox, :partition, :group_key],
        prefix: prefix,
        where: "status = 'pending' AND attempts > 0",
        name: "#{table}_retrying_index"
      )
    )

    # Pruning delivered rows and listing dead ones.
    create(
      index(table, [:outbox, :status, :delivered_at],
        prefix: prefix,
        where: "status <> 'pending'",
        name: "#{table}_settled_index"
      )
    )
  end

  defp change(1, :down, opts) do
    drop(table(table_name(opts), prefix: Keyword.get(opts, :prefix)))
  end

  defp table_name(opts), do: Keyword.get(opts, :table, "outboxer_messages")
end
