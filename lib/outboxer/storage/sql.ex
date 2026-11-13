defmodule Outboxer.Storage.SQL do
  @moduledoc false
  # The Ecto queries shared by the Postgres and SQLite adapters. Everything
  # here is portable SQL; what differs between the two (locking, notification)
  # lives in the adapter modules.

  import Ecto.Query

  alias Outboxer.Config
  alias Outboxer.Storage.Record

  @max_error_length 2_000

  def insert(%Config{} = conf, rows) do
    now = DateTime.utc_now()

    entries =
      Enum.map(rows, fn row ->
        %{
          outbox: Atom.to_string(row.outbox),
          partition: row.partition,
          group_key: row.group_key,
          type: row.type,
          payload: row.payload,
          headers: Record.encode_headers(row.headers),
          status: "pending",
          attempts: 0,
          scheduled_at: now,
          inserted_at: now
        }
      end)

    {_count, inserted} =
      conf.repo.insert_all({conf.table, Record}, entries, opts(conf, returning: [:id]))

    # insert_all returns rows in insertion order on both adapters.
    inserted
    |> Enum.zip(rows)
    |> Enum.map(fn {%{id: id}, row} ->
      %Outboxer.Envelope{
        id: id,
        outbox: row.outbox,
        partition: row.partition,
        group_key: row.group_key,
        type: row.type,
        payload: row.payload,
        headers: row.headers,
        inserted_at: now
      }
    end)
  end

  def fetch(%Config{} = conf, outbox, partition, limit, now) do
    name = Atom.to_string(outbox)

    # Groups whose head failed and is waiting for its retry. Nothing behind
    # that head may be delivered, or order would break.
    blocked =
      from(b in source(conf),
        where: b.outbox == ^name and b.partition == ^partition and b.status == "pending",
        where: b.attempts > 0 and b.scheduled_at > ^now and not is_nil(b.group_key),
        select: b.group_key
      )

    query =
      from(m in source(conf),
        where: m.outbox == ^name and m.partition == ^partition and m.status == "pending",
        where: m.scheduled_at <= ^now,
        where: is_nil(m.group_key) or m.group_key not in subquery(blocked),
        order_by: [asc: m.id],
        limit: ^limit
      )

    query
    |> conf.repo.all(opts(conf))
    |> Enum.map(&Record.to_envelope(&1, outbox))
  end

  def ack(_conf, [], _retention, _now), do: :ok

  def ack(%Config{} = conf, ids, :delete, _now) do
    conf.repo.delete_all(from(m in source(conf), where: m.id in ^ids), opts(conf))
    :ok
  end

  def ack(%Config{} = conf, ids, {:keep, _seconds}, now) do
    query = from(m in source(conf), where: m.id in ^ids)

    conf.repo.update_all(
      query,
      [set: [status: "delivered", delivered_at: now, last_error: nil]],
      opts(conf)
    )

    :ok
  end

  def fail(_conf, [], _retry_at, _error), do: :ok

  def fail(%Config{} = conf, ids, retry_at, error) do
    query = from(m in source(conf), where: m.id in ^ids)
    updates = [inc: [attempts: 1], set: [scheduled_at: retry_at, last_error: truncate(error)]]
    conf.repo.update_all(query, updates, opts(conf))
    :ok
  end

  def kill(_conf, [], _error, _now), do: :ok

  def kill(%Config{} = conf, ids, error, now) do
    query = from(m in source(conf), where: m.id in ^ids)

    updates = [
      inc: [attempts: 1],
      set: [status: "dead", delivered_at: now, last_error: truncate(error)]
    ]

    conf.repo.update_all(query, updates, opts(conf))
    :ok
  end

  def prune(%Config{} = conf, outbox, cutoff, limit) do
    name = Atom.to_string(outbox)

    stale =
      from(m in source(conf),
        where: m.outbox == ^name and m.status == "delivered" and m.delivered_at < ^cutoff,
        select: m.id,
        limit: ^limit
      )

    {count, _rows} =
      conf.repo.delete_all(from(m in source(conf), where: m.id in subquery(stale)), opts(conf))

    count
  end

  # -- admin queries, used by the Outboxer module -----------------------------

  def list(%Config{} = conf, outbox, status, limit) do
    name = Atom.to_string(outbox)

    query =
      from(m in source(conf),
        where: m.outbox == ^name and m.status == ^status,
        order_by: [asc: m.id],
        limit: ^limit
      )

    conf.repo.all(query, opts(conf))
  end

  def counts(%Config{} = conf, outbox) do
    name = Atom.to_string(outbox)

    query =
      from(m in source(conf),
        where: m.outbox == ^name,
        group_by: m.status,
        select: {m.status, count(m.id)}
      )

    counts = query |> conf.repo.all(opts(conf)) |> Map.new()

    %{
      pending: Map.get(counts, "pending", 0),
      delivered: Map.get(counts, "delivered", 0),
      dead: Map.get(counts, "dead", 0)
    }
  end

  def revive(%Config{} = conf, outbox, ids) do
    name = Atom.to_string(outbox)
    query = from(m in source(conf), where: m.outbox == ^name and m.status == "dead")
    query = if ids == :all, do: query, else: from(m in query, where: m.id in ^ids)

    updates = [
      set: [status: "pending", attempts: 0, scheduled_at: DateTime.utc_now(), delivered_at: nil]
    ]

    {count, _rows} = conf.repo.update_all(query, updates, opts(conf))
    count
  end

  def discard(%Config{} = conf, ids) do
    {count, _rows} =
      conf.repo.delete_all(from(m in source(conf), where: m.id in ^ids), opts(conf))

    count
  end

  # -- helpers ----------------------------------------------------------------

  def source(%Config{table: table}), do: {table, Record}

  def opts(%Config{prefix: nil}, extra), do: extra
  def opts(%Config{prefix: prefix}, extra), do: Keyword.put(extra, :prefix, prefix)
  def opts(conf), do: opts(conf, [])

  defp truncate(error) when byte_size(error) > @max_error_length do
    String.slice(error, 0, @max_error_length)
  end

  defp truncate(error), do: error
end
