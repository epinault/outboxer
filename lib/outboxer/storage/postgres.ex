if Code.ensure_loaded?(Postgrex) do
  defmodule Outboxer.Storage.Postgres do
    @moduledoc """
    Postgres storage. Safe to run on any number of nodes.

    ## How a batch is delivered

        BEGIN
        SELECT pg_try_advisory_xact_lock(outbox_key, partition)   -- or give up: another node has it
        SELECT ... WHERE status = 'pending' ... ORDER BY id LIMIT batch_size
        -- relay delivers the batch
        DELETE ...                                                  -- or mark delivered / failed
        COMMIT

    The advisory lock is scoped to the transaction, so it is released when the
    transaction ends for any reason, including the worker or the node dying.
    Nothing has to expire and no row is ever "claimed" with an `UPDATE`, which
    is why a delivered message costs one `INSERT` and one `DELETE`.

    Transaction-scoped locks also work behind PgBouncer in transaction mode.

    ## Things to know

      * The transaction stays open while the relay talks to the broker, so each
        busy partition holds one connection from the repo pool. Size the pool
        for `sum of partitions` on top of your normal load, and keep
        `batch_size` and relay timeouts modest.
      * The repo must use the default `READ COMMITTED` isolation level.
      * With `notify: true`, publishing issues `pg_notify`, which Postgres
        delivers only if and when the publishing transaction commits.
    """

    @behaviour Outboxer.Storage

    alias Outboxer.Config
    alias Outboxer.Storage.SQL

    @channel "outboxer"

    @doc false
    def channel, do: @channel

    @impl Outboxer.Storage
    defdelegate insert(conf, rows), to: SQL

    @impl Outboxer.Storage
    defdelegate fetch(conf, outbox, partition, limit, now), to: SQL

    @impl Outboxer.Storage
    defdelegate ack(conf, ids, retention, now), to: SQL

    @impl Outboxer.Storage
    defdelegate fail(conf, ids, retry_at, error), to: SQL

    @impl Outboxer.Storage
    defdelegate kill(conf, ids, error, now), to: SQL

    @impl Outboxer.Storage
    defdelegate prune(conf, outbox, cutoff, limit), to: SQL

    @impl Outboxer.Storage
    def lock_group(%Config{} = conf, outbox, group_key) do
      key = :erlang.phash2({conf.prefix, conf.table, outbox, group_key}, 2_147_483_647)
      conf.repo.query!("SELECT pg_advisory_xact_lock($1, $2)", [lock_key(conf, outbox), key])
      :ok
    end

    @impl Outboxer.Storage
    def with_partition(%Config{} = conf, outbox, partition, fun) do
      {:ok, result} =
        conf.repo.transaction(fn ->
          case conf.repo.query!("SELECT pg_try_advisory_xact_lock($1, $2)", [
                 lock_key(conf, outbox),
                 partition
               ]) do
            %{rows: [[true]]} -> {:ok, fun.()}
            %{rows: [[false]]} -> :busy
          end
        end)

      result
    end

    @impl Outboxer.Storage
    def notify(%Config{} = conf, outbox, partition) do
      conf.repo.query!("SELECT pg_notify($1, $2)", [
        @channel,
        "#{conf.name}|#{outbox}|#{partition}"
      ])

      :ok
    end

    # The two-argument advisory lock functions take two int4 values. The first
    # identifies the outbox (within its table), the second the partition.
    defp lock_key(%Config{} = conf, outbox) do
      :erlang.phash2({conf.prefix, conf.table, outbox}, 2_147_483_647)
    end
  end
end
