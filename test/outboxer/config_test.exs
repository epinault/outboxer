defmodule Outboxer.ConfigTest do
  use ExUnit.Case, async: true

  alias Outboxer.Config
  alias Outboxer.Test.Sink
  alias Outboxer.Test.SqliteRepo

  defp new!(outbox_opts, opts \\ []) do
    Config.new!(
      [repo: SqliteRepo, outboxes: [events: Keyword.put_new(outbox_opts, :relay, Sink)]] ++ opts
    )
  end

  test "applies defaults" do
    conf = new!([])

    assert conf.name == Outboxer
    assert conf.table == "outboxer_messages"
    assert conf.storage == Outboxer.Storage.SQLite
    assert conf.testing == :disabled

    assert %{
             ordering: :group,
             partitions: 1,
             batch_size: 100,
             poll_interval: 1_000,
             max_attempts: :infinity,
             retention: :delete,
             backoff: [min: 1_000, max: 60_000]
           } = conf.outboxes.events
  end

  test "picks the storage from the repo adapter" do
    assert Outboxer.Storage.for_repo!(SqliteRepo) == Outboxer.Storage.SQLite
    assert Outboxer.Storage.for_repo!(Outboxer.Test.PgRepo) == Outboxer.Storage.Postgres

    assert_raise ArgumentError, ~r/expected :repo to be an Ecto.Repo/, fn ->
      Outboxer.Storage.for_repo!(String)
    end
  end

  test "initialises the relay with its options" do
    conf = new!(relay: {Sink, pid: self(), mode: :batch})
    assert conf.outboxes.events.relay == Sink
    assert conf.outboxes.events.relay_state == %{pid: self(), mode: :batch}
  end

  test "ordering: :total forces a single partition" do
    assert new!(ordering: :total, partitions: 8).outboxes.events.partitions == 1
  end

  test "requires a repo and a relay" do
    assert_raise NimbleOptions.ValidationError, ~r/required :repo option not found/, fn ->
      Config.new!([])
    end

    assert_raise NimbleOptions.ValidationError, ~r/required :relay option not found/, fn ->
      Config.new!(repo: SqliteRepo, outboxes: [events: []])
    end
  end

  test "rejects invalid outbox options with the option named" do
    assert_raise NimbleOptions.ValidationError, ~r/:ordering/, fn ->
      new!(ordering: :sometimes)
    end

    assert_raise NimbleOptions.ValidationError, ~r/:partitions/, fn -> new!(partitions: 0) end
    assert_raise NimbleOptions.ValidationError, ~r/:max_attempts/, fn -> new!(max_attempts: 0) end

    assert_raise NimbleOptions.ValidationError, ~r/:retention/, fn ->
      new!(retention: :forever)
    end

    assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:colour\]/, fn ->
      new!(colour: :red)
    end

    assert_raise ArgumentError, ~r/invalid :backoff for outbox :events/, fn ->
      new!(backoff: [min: 10, max: 1])
    end
  end

  test "rejects a relay that does not exist, with a hint for the built-in ones" do
    assert_raise ArgumentError, ~r/relay NoSuchRelay of outbox :events is not available/, fn ->
      new!(relay: NoSuchRelay)
    end

    assert_raise ArgumentError, ~r/does not implement Outboxer.Relay/, fn ->
      new!(relay: String)
    end
  end

  test "surfaces the relay's own option errors" do
    assert_raise ArgumentError, ~r/rejected its options: .*:fun/, fn ->
      new!(relay: {Outboxer.Relay.Function, []})
    end

    assert_raise ArgumentError, ~r/rejected its options: required :topic option not found/, fn ->
      new!(relay: {Outboxer.Relay.Kafka, client: :c})
    end
  end

  test "fetch! and outbox! explain what is missing" do
    assert_raise ArgumentError, ~r/no Outboxer instance named Missing is running/, fn ->
      Config.fetch!(Missing)
    end

    assert_raise ArgumentError,
                 ~r/unknown outbox :nope .* Configured outboxes: \[:events\]/,
                 fn ->
                   Config.outbox!(new!([]), :nope)
                 end
  end
end
