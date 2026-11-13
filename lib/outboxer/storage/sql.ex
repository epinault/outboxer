defmodule Outboxer.Storage.SQL do
  @moduledoc false
  # The Ecto queries shared by the Postgres and SQLite adapters. Everything
  # here is portable SQL; what differs between the two (locking, notification)
  # lives in the adapter modules.

  import Ecto.Query

  alias Outboxer.Config
  alias Outboxer.Storage.Record

  @max_error_length 2_000

  # Rows may belong to outboxes in different tables: one INSERT per table,
  # and the envelopes come back in the order the rows were given.
  def insert(%Config{} = conf, rows) do
    now = DateTime.utc_now()

    rows
    |> Enum.with_index()
    |> Enum.group_by(fn {row, _index} -> Config.outbox!(conf, row.outbox).table end)
    |> Enum.flat_map(fn {table, indexed} -> insert_into(conf, table, indexed, now) end)
    |> Enum.sort_by(fn {_envelope, index} -> index end)
    |> Enum.map(fn {envelope, _index} -> envelope end)
  end

  defp insert_into(conf, table, indexed, now) do
    entries =
      Enum.map(indexed, fn {row, _index} ->
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
      conf.repo.insert_all({table, Record}, entries, opts(conf, returning: [:id]))

    # insert_all returns rows in insertion order on both adapters.
    inserted
    |> Enum.zip(indexed)
    |> Enum.map(fn {%{id: id}, {row, index}} ->
      envelope = %Outboxer.Envelope{
        id: id,
        outbox: row.outbox,
        partition: row.partition,
        group_key: row.group_key,
        type: row.type,
        payload: row.payload,
        headers: row.headers,
        inserted_at: now
      }

      {envelope, index}
    end)
  end

  def fetch(%Config{} = conf, outbox, partition, limit, now) do
    conf
    |> fetch_query(outbox, partition, limit, now)
    |> conf.repo.all(opts(conf))
    |> Enum.map(&Record.to_envelope(&1, outbox))
  end

  @doc "The query behind `fetch/5`. Public so that the benchmark can EXPLAIN it."
  def fetch_query(%Config{} = conf, outbox, partition, limit, now) do
    name = Atom.to_string(outbox)

    # Groups whose head failed and is waiting for its retry. Nothing behind
    # that head may be delivered, or order would break.
    blocked =
      from(b in source(conf, outbox),
        where: b.outbox == ^name and b.partition == ^partition and b.status == "pending",
        where: b.attempts > 0 and b.scheduled_at > ^now and not is_nil(b.group_key),
        select: b.group_key
      )

    # The partition is matched as a range and repeated in the ORDER BY on
    # purpose. With `partition = $1 ORDER BY id`, Postgres may walk the primary
    # key instead of the partial pending index: equally good while every row is
    # pending, but it then has to step over every delivered or dead row, and it
    # keeps that plan until the table is next analyzed. Written this way only
    # the pending index can return rows in order, whatever the statistics say.
    from(m in source(conf, outbox),
      where: m.outbox == ^name and m.status == "pending",
      where: m.partition >= ^partition and m.partition <= ^partition,
      where: m.scheduled_at <= ^now,
      where: is_nil(m.group_key) or m.group_key not in subquery(blocked),
      order_by: [asc: m.partition, asc: m.id],
      limit: ^limit
    )
  end

  def ack(_conf, _outbox, [], _retention, _now), do: :ok

  def ack(%Config{} = conf, outbox, ids, :delete, _now) do
    conf.repo.delete_all(from(m in source(conf, outbox), where: m.id in ^ids), opts(conf))
    :ok
  end

  def ack(%Config{} = conf, outbox, ids, {:keep, _seconds}, now) do
    query = from(m in source(conf, outbox), where: m.id in ^ids)

    conf.repo.update_all(
      query,
      [set: [status: "delivered", delivered_at: now, last_error: nil]],
      opts(conf)
    )

    :ok
  end

  def fail(_conf, _outbox, [], _retry_at, _error), do: :ok

  def fail(%Config{} = conf, outbox, ids, retry_at, error) do
    query = from(m in source(conf, outbox), where: m.id in ^ids)
    updates = [inc: [attempts: 1], set: [scheduled_at: retry_at, last_error: truncate(error)]]
    conf.repo.update_all(query, updates, opts(conf))
    :ok
  end

  def kill(_conf, _outbox, [], _error, _now), do: :ok

  def kill(%Config{} = conf, outbox, ids, error, now) do
    query = from(m in source(conf, outbox), where: m.id in ^ids)

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
      from(m in source(conf, outbox),
        where: m.outbox == ^name and m.status == "delivered" and m.delivered_at < ^cutoff,
        select: m.id,
        limit: ^limit
      )

    {count, _rows} =
      conf.repo.delete_all(
        from(m in source(conf, outbox), where: m.id in subquery(stale)),
        opts(conf)
      )

    count
  end

  # -- admin queries, used by the Outboxer module -----------------------------

  def list(%Config{} = conf, outbox, status, limit) do
    name = Atom.to_string(outbox)

    query =
      from(m in source(conf, outbox),
        where: m.outbox == ^name and m.status == ^status,
        order_by: [asc: m.id],
        limit: ^limit
      )

    conf.repo.all(query, opts(conf))
  end

  def counts(%Config{} = conf, outbox) do
    name = Atom.to_string(outbox)

    query =
      from(m in source(conf, outbox),
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
    query = from(m in source(conf, outbox), where: m.outbox == ^name and m.status == "dead")
    query = if ids == :all, do: query, else: from(m in query, where: m.id in ^ids)

    updates = [
      set: [status: "pending", attempts: 0, scheduled_at: DateTime.utc_now(), delivered_at: nil]
    ]

    {count, _rows} = conf.repo.update_all(query, updates, opts(conf))
    count
  end

  def discard(%Config{} = conf, outbox, ids) do
    name = Atom.to_string(outbox)
    query = from(m in source(conf, outbox), where: m.outbox == ^name and m.id in ^ids)
    {count, _rows} = conf.repo.delete_all(query, opts(conf))

    count
  end

  # -- helpers ----------------------------------------------------------------

  @doc "The Ecto source of an outbox: its table, read through the `Record` schema."
  def source(%Config{} = conf, outbox), do: {Config.outbox!(conf, outbox).table, Record}

  def opts(%Config{prefix: nil}, extra), do: extra
  def opts(%Config{prefix: prefix}, extra), do: Keyword.put(extra, :prefix, prefix)
  def opts(conf), do: opts(conf, [])

  defp truncate(error) when byte_size(error) > @max_error_length do
    String.slice(error, 0, @max_error_length)
  end

  defp truncate(error), do: error
end
