# Outboxer benchmark.
#
#   docker compose up -d postgres
#   mix bench                       # Postgres (the default)
#   BENCH_REPO=sqlite mix bench     # SQLite
#   BENCH_SCALE=5 mix bench         # five times the rows
#   BENCH_PLANS=1 mix bench         # also print the query plans (Postgres)
#
# It measures the library, not a broker: the relay is a no-op that counts.
# Numbers depend on the machine; what matters is how they change with the
# shape of the data (deep backlog, blocked groups, churn).

defmodule Bench.Msg do
  use Outboxer.Message, outbox: :bench

  defstruct [:n, :group]

  @impl Outboxer.Message
  def group_key(%__MODULE__{group: group}), do: group
end

defmodule Bench.Relay do
  @behaviour Outboxer.Relay

  @impl Outboxer.Relay
  def init(opts), do: {:ok, Map.new(opts)}

  @impl Outboxer.Relay
  def deliver(envelopes, %{counter: counter}) do
    :counters.add(counter, 1, length(envelopes))
    :ok
  end
end

defmodule Bench.Migration do
  use Ecto.Migration

  def up, do: Outboxer.Migration.up(table: "bench_outbox")
  def down, do: Outboxer.Migration.down(table: "bench_outbox")
end

defmodule Bench do
  alias Outboxer.Config
  alias Outboxer.Storage.Record
  alias Outboxer.Storage.SQL

  @table "bench_outbox"
  @payload String.duplicate("x", 200)

  def run do
    repo = repo()
    {:ok, _pid} = repo.start_link()
    migrate(repo)
    scale = String.to_integer(System.get_env("BENCH_SCALE", "1"))
    ctx = %{repo: repo, scale: scale, pg?: repo.__adapter__() == Ecto.Adapters.Postgres}

    IO.puts("\nOutboxer benchmark on #{inspect(repo.__adapter__())}, scale #{scale}\n")

    publish(ctx)
    drain(ctx)
    backlog(ctx)
    blocked(ctx)
    churn(ctx)
    prune(ctx)

    truncate(ctx)
    IO.puts("")
  end

  # -- scenarios ---------------------------------------------------------------

  defp publish(ctx) do
    section("Publishing (business transaction + outbox insert)")
    n = 5_000 * ctx.scale

    for {label, writers, opts, outbox_opts} <- [
          {"1 message per transaction, 1 writer", 1, [], []},
          {"1 message per transaction, 10 writers", 10, [], []},
          {"same, with notify: true", 10, [notify: true], []},
          {"same, with serialize_writes: true", 10, [], [serialize_writes: true]}
        ],
        ctx.pg? or (opts == [] and outbox_opts == []) do
      truncate(ctx)
      name = start(ctx, [relay: false] ++ opts, outbox_opts)

      us =
        time(fn ->
          each_writer(writers, fn writer ->
            for i <- 1..div(n, writers) do
              ctx.repo.transaction(fn ->
                Outboxer.publish(%Bench.Msg{n: i, group: "g#{writer}-#{rem(i, 50)}"}, name: name)
              end)
            end
          end)
        end)

      stop(name)
      rate(label, n, us, "txn/s")
    end

    truncate(ctx)
    name = start(ctx, [relay: false], [])
    batches = div(n, 10)

    us =
      time(fn ->
        for b <- 1..batches do
          messages = for i <- 1..100, do: %Bench.Msg{n: i, group: "g#{rem(b, 50)}"}
          ctx.repo.transaction(fn -> Outboxer.publish(messages, name: name) end)
        end
      end)

    stop(name)
    rate("100 messages per transaction, 1 writer", batches * 100, us, "msg/s")
  end

  defp drain(ctx) do
    section("Relaying a backlog (no-op relay, batch_size 100)")
    n = 200_000 * ctx.scale

    for {label, seed_opts, outbox_opts} <- [
          {"1 partition, 1,000 groups", [groups: 1_000], []},
          {"4 partitions, 1,000 groups", [groups: 1_000, partitions: 4], [partitions: 4]},
          {"1 partition, no groups", [], [ordering: :none]},
          {"1 partition, batch_size 1,000", [groups: 1_000], [batch_size: 1_000]},
          {"1 partition, retention {:keep, _}", [groups: 1_000], [retention: {:keep, 3_600}]}
        ] do
      truncate(ctx)
      seed(ctx, n, seed_opts)
      analyze(ctx)
      {us, first, last} = relay_all(ctx, n, outbox_opts)

      rate(label, n, us, "msg/s", "first 20%: #{per_s(first)}/s, last 20%: #{per_s(last)}/s")
    end
  end

  defp backlog(ctx) do
    section("Fetching one batch from a deep backlog (latency of one poll)")

    for n <- [10_000, 100_000, 1_000_000] do
      n = n * ctx.scale
      truncate(ctx)
      seed(ctx, n, groups: 1_000)
      analyze(ctx)
      fetch_latency(ctx, "#{fmt(n)} pending rows", [])
    end

    name = start(ctx, [relay: false], [])
    us = time(fn -> Outboxer.counts(:bench, name: name) end)
    stop(name)
    line("counts/2 on that table", "#{ms(us)} ms")
  end

  defp blocked(ctx) do
    section("Fetching when groups are blocked (strict ordering, failed head)")
    behind = 100_000 * ctx.scale
    future = DateTime.add(DateTime.utc_now(), 3_600, :second)

    truncate(ctx)
    seed(ctx, 1, group: "poison", attempts: 1, scheduled_at: future)
    seed(ctx, behind, group: "poison")
    seed(ctx, 10_000, groups: 1_000)
    analyze(ctx)
    fetch_latency(ctx, "1 blocked group, #{fmt(behind)} rows queued behind its head", plan: true)

    truncate(ctx)
    seed(ctx, 2_000, groups: 2_000, prefix: "b", attempts: 1, scheduled_at: future)
    seed(ctx, 20_000, groups: 2_000, prefix: "b")
    seed(ctx, 10_000, groups: 1_000)
    analyze(ctx)
    fetch_latency(ctx, "2,000 blocked groups, 10 rows queued behind each", [])

    truncate(ctx)
    seed(ctx, 10_000, groups: 1_000)
    analyze(ctx)
    fetch_latency(ctx, "nothing blocked (baseline)", plan: true)
  end

  # Publishers and the relay running against each other: the steady state of a
  # busy outbox, where rows are inserted and deleted at the same rate.
  defp churn(ctx) do
    section("Churn: publishing and relaying at the same time")
    truncate(ctx)
    seconds = 10 * ctx.scale
    writers = if ctx.pg?, do: 8, else: 2
    counter = :counters.new(1, [:atomics])
    published = :counters.new(1, [:atomics])

    name =
      start(ctx, [], relay: {Bench.Relay, counter: counter}, poll_interval: 20, partitions: 2)

    deadline = System.monotonic_time(:millisecond) + seconds * 1_000

    each_writer(writers, fn writer ->
      publish_until(ctx, name, writer, published, deadline, 0)
    end)

    total = :counters.get(published, 1)
    lag = total - :counters.get(counter, 1)
    wait_for(counter, total)
    stop(name)

    line(
      "published in #{seconds} s by #{writers} writers",
      "#{fmt(total)} (#{fmt(div(total, seconds))}/s)"
    )

    line("backlog when publishing stopped", fmt(lag))

    if ctx.pg? do
      sizes(ctx, "table and indexes, table now empty, before vacuum")
      fetch_latency(ctx, "poll on the empty table, before vacuum", [])
      ctx.repo.query!("VACUUM #{@table}")
      sizes(ctx, "after VACUUM")
      fetch_latency(ctx, "poll on the empty table, after vacuum", [])
    end
  end

  defp prune(ctx) do
    section("Pruning delivered rows (retention: {:keep, _})")
    n = 200_000 * ctx.scale
    truncate(ctx)
    old = DateTime.add(DateTime.utc_now(), -7_200, :second)
    seed(ctx, n, groups: 1_000, status: "delivered", delivered_at: old)
    seed(ctx, 50_000, groups: 1_000)
    analyze(ctx)

    name = start(ctx, [relay: false], retention: {:keep, 60})
    conf = Config.fetch!(name)
    us = time(fn -> ^n = Outboxer.Pruner.prune(conf) end)
    stop(name)
    rate("#{fmt(n)} delivered rows, 50,000 pending left alone", n, us, "rows/s")
  end

  # -- measuring ---------------------------------------------------------------

  defp relay_all(ctx, n, outbox_opts) do
    counter = :counters.new(1, [:atomics])
    name = start(ctx, [], [relay: {Bench.Relay, counter: counter}] ++ outbox_opts)
    started = System.monotonic_time(:microsecond)
    samples = sample(counter, n, started, [])
    stop(name)

    {us, _count} = List.last(samples)
    {t20, c20} = Enum.find(samples, fn {_t, count} -> count >= n * 0.2 end)
    {t80, c80} = Enum.find(samples, fn {_t, count} -> count >= n * 0.8 end)
    {us, c20 / max(t20, 1) * 1_000_000, (n - c80) / max(us - t80, 1) * 1_000_000}
  end

  defp sample(counter, n, started, samples) do
    Process.sleep(20)
    point = {System.monotonic_time(:microsecond) - started, :counters.get(counter, 1)}

    if elem(point, 1) >= n,
      do: Enum.reverse([point | samples]),
      else: sample(counter, n, started, [point | samples])
  end

  defp wait_for(counter, n) do
    if :counters.get(counter, 1) < n do
      Process.sleep(20)
      wait_for(counter, n)
    end
  end

  defp fetch_latency(ctx, label, opts) do
    name = start(ctx, [relay: false], [])
    conf = Config.fetch!(name)
    fetch = fn -> conf.storage.fetch(conf, :bench, 0, 100, DateTime.utc_now()) end
    fetched = length(fetch.())
    times = for(_run <- 1..30, do: time(fetch)) |> Enum.sort()

    line(
      label,
      "median #{ms(Enum.at(times, 15))} ms, max #{ms(List.last(times))} ms (#{fetched} rows)"
    )

    if opts[:plan] && ctx.pg? && System.get_env("BENCH_PLANS") do
      query = SQL.fetch_query(conf, :bench, 0, 100, DateTime.utc_now())
      IO.puts("\n" <> ctx.repo.explain(:all, query, analyze: true, buffers: true) <> "\n")
    end

    stop(name)
  end

  defp publish_until(ctx, name, writer, published, deadline, i) do
    if System.monotonic_time(:millisecond) < deadline do
      ctx.repo.transaction(fn ->
        Outboxer.publish(%Bench.Msg{n: i, group: "g#{writer}-#{rem(i, 200)}"}, name: name)
      end)

      :counters.add(published, 1, 1)
      publish_until(ctx, name, writer, published, deadline, i + 1)
    end
  end

  defp each_writer(writers, fun) do
    1..writers
    |> Task.async_stream(fun, max_concurrency: writers, timeout: :infinity)
    |> Stream.run()
  end

  defp time(fun), do: fun |> :timer.tc() |> elem(0)

  # -- instance and data -------------------------------------------------------

  defp start(ctx, opts, outbox_opts) do
    name = Module.concat(Bench, "I#{System.unique_integer([:positive])}")
    outbox = Keyword.merge([table: @table], outbox_opts)

    {:ok, _pid} =
      Outboxer.start_link([name: name, repo: ctx.repo, outboxes: [bench: outbox]] ++ opts)

    name
  end

  defp stop(name), do: Supervisor.stop(name)

  # Rows are written directly: publishing a million messages through the API
  # would measure publishing, not what comes after.
  defp seed(%{pg?: true} = ctx, count, opts) do
    groups = Keyword.get(opts, :groups, 0)

    {group_sql, partition_sql} =
      cond do
        opts[:group] -> {"$4::text", "0"}
        groups > 0 -> {"$4::text || (g % $2::int)", "(g % $2::int) % $3::int"}
        true -> {"NULL", "g % $3::int"}
      end

    ctx.repo.query!(
      """
      INSERT INTO #{@table}
        (outbox, partition, group_key, type, payload, status, attempts, scheduled_at, inserted_at, delivered_at)
      SELECT 'bench', #{partition_sql}, #{group_sql}, 'bench.msg', $5::bytea, $6::text, $7::int,
             $8::timestamp, now(), $9::timestamp
      FROM generate_series(1, $1::int) g
      WHERE $2::int >= 0 AND $3::int > 0 AND $4::text IS NOT NULL
      """,
      [
        count,
        groups,
        Keyword.get(opts, :partitions, 1),
        opts[:group] || Keyword.get(opts, :prefix, "g"),
        @payload,
        Keyword.get(opts, :status, "pending"),
        Keyword.get(opts, :attempts, 0),
        naive(Keyword.get(opts, :scheduled_at, DateTime.utc_now())),
        naive(opts[:delivered_at])
      ],
      timeout: :infinity
    )
  end

  defp seed(ctx, count, opts) do
    groups = Keyword.get(opts, :groups, 0)
    partitions = Keyword.get(opts, :partitions, 1)
    prefix = Keyword.get(opts, :prefix, "g")
    now = DateTime.utc_now()

    1..count
    |> Stream.map(fn g ->
      {group, partition} =
        cond do
          opts[:group] -> {opts[:group], 0}
          groups > 0 -> {"#{prefix}#{rem(g, groups)}", rem(rem(g, groups), partitions)}
          true -> {nil, rem(g, partitions)}
        end

      %{
        outbox: "bench",
        partition: partition,
        group_key: group,
        type: "bench.msg",
        payload: @payload,
        status: Keyword.get(opts, :status, "pending"),
        attempts: Keyword.get(opts, :attempts, 0),
        scheduled_at: Keyword.get(opts, :scheduled_at, now),
        inserted_at: now,
        delivered_at: opts[:delivered_at]
      }
    end)
    |> Stream.chunk_every(2_000)
    |> Enum.each(&ctx.repo.insert_all({@table, Record}, &1, timeout: :infinity))
  end

  defp naive(nil), do: nil
  defp naive(%DateTime{} = datetime), do: DateTime.to_naive(datetime)

  defp truncate(%{pg?: true} = ctx), do: ctx.repo.query!("TRUNCATE #{@table} RESTART IDENTITY")
  defp truncate(ctx), do: ctx.repo.delete_all(@table)

  defp analyze(%{pg?: true} = ctx), do: ctx.repo.query!("VACUUM ANALYZE #{@table}")
  defp analyze(ctx), do: ctx.repo.query!("ANALYZE")

  defp sizes(ctx, label) do
    %{rows: [[table, indexes]]} =
      ctx.repo.query!(
        "SELECT pg_relation_size($1::text::regclass), pg_indexes_size($1::text::regclass)",
        [
          @table
        ]
      )

    line(label, "table #{kb(table)}, indexes #{kb(indexes)}")
  end

  defp migrate(repo) do
    repo.query!("DROP TABLE IF EXISTS #{@table}")

    try do
      repo.query!("DELETE FROM schema_migrations WHERE version = 9001")
    rescue
      _no_table -> :ok
    end

    Ecto.Migrator.up(repo, 9001, Bench.Migration, log: false)
  end

  defp repo do
    case System.get_env("BENCH_REPO", "postgres") do
      "postgres" -> Outboxer.Test.PgRepo
      "sqlite" -> Outboxer.Test.SqliteRepo
    end
  end

  # -- output ------------------------------------------------------------------

  defp section(title), do: IO.puts("\n#{title}\n#{String.duplicate("-", String.length(title))}")

  defp rate(label, count, us, unit, note \\ nil) do
    value = "#{per_s(count / us * 1_000_000)} #{unit}"
    line(label, if(note, do: "#{value}   (#{note})", else: value))
  end

  defp line(label, value), do: IO.puts("  #{String.pad_trailing(label, 58)} #{value}")

  defp per_s(rate), do: rate |> round() |> fmt()
  defp ms(us), do: :erlang.float_to_binary(us / 1_000, decimals: 2)
  defp kb(bytes), do: "#{fmt(div(bytes, 1_024))} kB"

  defp fmt(integer) do
    integer
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end
end

Bench.run()
