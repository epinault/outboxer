if Code.ensure_loaded?(Ecto.Adapters.SQLite3) do
  defmodule Outboxer.Storage.SQLite do
    @moduledoc """
    SQLite storage, for single-node applications.

    SQLite allows one writer at a time, which shapes two decisions:

      * **Delivery happens outside any transaction.** Holding the write lock
        while a relay talks to a broker would stall every other write in your
        application. A batch is read, delivered, and then removed with a single
        short statement.
      * **Exclusivity comes from the supervision tree.** There is exactly one
        worker process per partition on the node, and a SQLite database belongs
        to one node, so no database lock is needed.

    The consequence: do not point two running nodes (or two Outboxer instances
    with the same outboxes) at the same SQLite file. They would deliver the
    same messages twice and out of order. For a replicated setup such as
    LiteFS, run the relay on the primary only and start the other nodes with
    `relay: false` so they can still publish.

    `serialize_writes` and `notify` have no effect here: writers are already
    serialised by SQLite, and there are no other nodes to notify.
    """

    @behaviour Outboxer.Storage

    alias Outboxer.Storage.SQL

    @impl Outboxer.Storage
    defdelegate insert(conf, rows), to: SQL

    @impl Outboxer.Storage
    defdelegate fetch(conf, outbox, partition, limit, now), to: SQL

    @impl Outboxer.Storage
    defdelegate ack(conf, outbox, ids, retention, now), to: SQL

    @impl Outboxer.Storage
    defdelegate fail(conf, outbox, ids, retry_at, error), to: SQL

    @impl Outboxer.Storage
    defdelegate kill(conf, outbox, ids, error, now), to: SQL

    @impl Outboxer.Storage
    defdelegate prune(conf, outbox, cutoff, limit), to: SQL

    @impl Outboxer.Storage
    def lock_group(_conf, _outbox, _group_key), do: :ok

    @impl Outboxer.Storage
    def with_partition(_conf, _outbox, _partition, fun), do: {:ok, fun.()}
  end
end
