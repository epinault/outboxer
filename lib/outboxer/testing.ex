defmodule Outboxer.Testing do
  @moduledoc """
  Helpers for testing code that publishes to an outbox.

  Start Outboxer with `testing: :manual` in your test environment. No workers
  run, so nothing is relayed behind your back and the Ecto SQL sandbox needs no
  special arrangement: `Outboxer.publish/2` writes through the test's own
  connection and the rows roll back with the test.

      # config/test.exs
      config :my_app, :outboxer, testing: :manual

      # application.ex
      {Outboxer, [repo: MyApp.Repo, outboxes: [...]] ++ Application.get_env(:my_app, :outboxer, [])}

  Then, in a test:

      import Outboxer.Testing

      test "placing an order publishes an event" do
        {:ok, order} = Orders.place(params)

        envelope = assert_published(:orders, MyApp.Events.OrderPlaced)
        assert JSON.decode!(envelope.payload)["order_id"] == order.id
      end

  To exercise the relay too, call `drain/2`, which delivers synchronously in
  the test process.
  """

  alias Outboxer.Config
  alias Outboxer.Dispatcher

  @compile {:no_warn_undefined, ExUnit.AssertionError}

  @doc """
  Returns the pending messages of an outbox, oldest first.

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
    * `:limit` - maximum number of messages, default `100`.
  """
  @spec published(atom(), keyword()) :: [Outboxer.Envelope.t()]
  def published(outbox, opts \\ []), do: Outboxer.list_pending(outbox, opts)

  @doc """
  Asserts that a pending message matching `matcher` exists and returns the
  first one.

  `matcher` is a message module (matches on its type), a type string, or a
  function taking the envelope and returning a boolean.

      assert_published(:orders, MyApp.Events.OrderPlaced)
      assert_published(:orders, "order.placed")
      assert_published(:orders, &(&1.group_key == "order:1"))
  """
  @spec assert_published(
          atom(),
          module() | String.t() | (Outboxer.Envelope.t() -> boolean()),
          keyword()
        ) ::
          Outboxer.Envelope.t()
  def assert_published(outbox, matcher, opts \\ []) do
    envelopes = published(outbox, opts)

    case Enum.find(envelopes, matcher_fun(matcher)) do
      nil ->
        raise ExUnit.AssertionError,
          message:
            "expected a message matching #{describe(matcher)} in outbox #{inspect(outbox)}, " <>
              "found: #{inspect(Enum.map(envelopes, & &1.type))}"

      envelope ->
        envelope
    end
  end

  @doc "Asserts that no pending message matches `matcher`. See `assert_published/3`."
  @spec refute_published(
          atom(),
          module() | String.t() | (Outboxer.Envelope.t() -> boolean()),
          keyword()
        ) :: :ok
  def refute_published(outbox, matcher, opts \\ []) do
    case Enum.find(published(outbox, opts), matcher_fun(matcher)) do
      nil ->
        :ok

      envelope ->
        raise ExUnit.AssertionError,
          message:
            "expected no message matching #{describe(matcher)} in outbox #{inspect(outbox)}, " <>
              "found: #{inspect(envelope)}"
    end
  end

  @doc """
  Delivers everything currently deliverable in an outbox, synchronously, in
  the calling process, through the configured relay.

  Returns `%{delivered: n, failed: n, dead: n}`. Failed messages are scheduled
  for a retry in the future and are not attempted again by the same call.

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
  """
  @spec drain(atom(), keyword()) :: %{
          delivered: non_neg_integer(),
          failed: non_neg_integer(),
          dead: non_neg_integer()
        }
  def drain(outbox, opts \\ []) do
    conf = Config.fetch!(Keyword.get(opts, :name, Outboxer))
    outbox = Config.outbox!(conf, outbox)

    Enum.reduce(0..(outbox.partitions - 1), %{delivered: 0, failed: 0, dead: 0}, fn partition,
                                                                                    totals ->
      drain_partition(conf, outbox, partition, totals)
    end)
  end

  defp drain_partition(conf, outbox, partition, totals) do
    case Dispatcher.run(conf, outbox, partition) do
      {:ok, %{fetched: 0}} ->
        totals

      {:ok, summary} ->
        totals =
          Map.merge(totals, Map.take(summary, [:delivered, :failed, :dead]), fn _key, a, b ->
            a + b
          end)

        drain_partition(conf, outbox, partition, totals)

      :busy ->
        totals
    end
  end

  defp matcher_fun(fun) when is_function(fun, 1), do: fun
  defp matcher_fun(type) when is_binary(type), do: &(&1.type == type)
  defp matcher_fun(module) when is_atom(module), do: &(&1.type == module.type())

  defp describe(fun) when is_function(fun), do: "the given function"
  defp describe(other), do: inspect(other)
end
