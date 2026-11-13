defmodule Outboxer.Case do
  @moduledoc false
  # Test case for everything that touches the database.
  #
  # Tests are parameterised over the available repos (see `repos/0`), so the
  # same test runs against SQLite and Postgres. They do not use the SQL
  # sandbox: workers run in their own processes and the Postgres adapter
  # relies on real transactions and advisory locks. Instead every test starts
  # from an empty table and the modules run one at a time.

  use ExUnit.CaseTemplate

  alias Outboxer.Test.Sink

  using do
    quote do
      import Outboxer.Case

      alias Outboxer.Test.Event
    end
  end

  setup context do
    repo = Map.get(context, :repo)
    if repo, do: repo.delete_all("outboxer_messages")
    :ok
  end

  @doc "Parameters for `use ExUnit.Case, parameterize: Outboxer.Case.repos()`."
  def repos do
    for repo <- Application.get_env(:outboxer, :test_repos, []), do: %{repo: repo}
  end

  @doc """
  Starts a supervised Outboxer instance with a unique name and returns the name.

  Every outbox gets `Outboxer.Test.Sink` as its relay, reporting to the test
  process, unless it names its own.
  """
  def start_outboxer(context, outboxes, opts \\ []) do
    name = Module.concat(Outboxer.TestInstance, "N#{System.unique_integer([:positive])}")
    test_pid = self()

    outboxes =
      for {outbox, outbox_opts} <- outboxes do
        {sink_opts, outbox_opts} = Keyword.split(outbox_opts, [:fail, :mode])
        relay = {Sink, [pid: test_pid] ++ sink_opts}
        {outbox, Keyword.put_new(outbox_opts, :relay, relay)}
      end

    opts = Keyword.merge([name: name, repo: context.repo, outboxes: outboxes], opts)
    start_supervised!({Outboxer, opts}, id: name)
    name
  end

  @doc "Starts an agent holding the failure predicate used by `Outboxer.Test.Sink`."
  def start_failures(fun \\ fn _envelope -> false end) do
    start_supervised!({Agent, fn -> fun end}, id: {:failures, System.unique_integer()})
  end

  def set_failures(agent, fun), do: Agent.update(agent, fn _old -> fun end)

  @doc "Collects `count` delivered envelopes, failing the test on timeout."
  def collect(count, timeout \\ 5_000) do
    for _index <- 1..count//1 do
      receive do
        {:delivered, envelope} -> envelope
      after
        timeout ->
          ExUnit.Assertions.flunk("expected #{count} deliveries, timed out after #{timeout}ms")
      end
    end
  end

  @doc "Polls until `fun` returns a truthy value."
  def eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      result = fun.() ->
        result

      System.monotonic_time(:millisecond) > deadline ->
        ExUnit.Assertions.flunk("condition was not met in time")

      true ->
        Process.sleep(20)
        do_eventually(fun, deadline)
    end
  end

  def n(envelope), do: JSON.decode!(envelope.payload)["n"]
end
