defmodule Outboxer.Config.Outbox do
  @moduledoc false
  # Validated configuration of a single outbox, including its initialised relay.

  @enforce_keys [:name, :relay, :relay_state]
  defstruct [
    :name,
    :relay,
    :relay_state,
    ordering: :group,
    partitions: 1,
    batch_size: 100,
    poll_interval: 1_000,
    max_attempts: :infinity,
    backoff: [min: 1_000, max: 60_000],
    retention: :delete,
    serialize_writes: false
  ]

  @type t :: %__MODULE__{
          name: atom(),
          relay: module(),
          relay_state: term(),
          ordering: :group | :total | :none,
          partitions: pos_integer(),
          batch_size: pos_integer(),
          poll_interval: pos_integer(),
          max_attempts: :infinity | pos_integer(),
          backoff: keyword() | (pos_integer() -> non_neg_integer()),
          retention: :delete | {:keep, pos_integer()},
          serialize_writes: boolean()
        }

  @total_group "*"

  @doc "The group key every message of an `ordering: :total` outbox is stored under."
  def total_group, do: @total_group

  @spec new!(atom(), keyword()) :: t()
  def new!(name, opts) do
    {relay, relay_opts} = split_relay(Keyword.fetch!(opts, :relay))
    validate_backoff!(name, Keyword.fetch!(opts, :backoff))

    fields =
      opts
      |> Keyword.drop([:relay])
      |> Keyword.merge(
        name: name,
        relay: relay,
        relay_state: init_relay!(name, relay, relay_opts)
      )

    __MODULE__ |> struct!(fields) |> normalize()
  end

  # A single global order means a single lane.
  defp normalize(%__MODULE__{ordering: :total} = outbox), do: %{outbox | partitions: 1}
  defp normalize(outbox), do: outbox

  defp split_relay({module, opts}), do: {module, opts}
  defp split_relay(module) when is_atom(module), do: {module, []}

  defp init_relay!(name, relay, relay_opts) do
    if not (Code.ensure_loaded?(relay) and function_exported?(relay, :deliver, 2)) do
      raise ArgumentError, relay_missing_message(name, relay)
    end

    case relay.init(relay_opts) do
      {:ok, state} ->
        state

      {:error, reason} ->
        raise ArgumentError,
              "relay #{inspect(relay)} of outbox #{inspect(name)} rejected its options: #{format(reason)}"
    end
  end

  defp relay_missing_message(name, relay) do
    hint =
      case relay do
        Outboxer.Relay.Kafka ->
          " Add {:brod, \"~> 4.0\"} to your deps and recompile outboxer."

        Outboxer.Relay.SQS ->
          " Add {:ex_aws_sqs, \"~> 3.4\"} to your deps and recompile outboxer."

        Outboxer.Relay.Oban ->
          " Add {:oban, \"~> 2.18\"} to your deps and recompile outboxer."

        _other ->
          ""
      end

    "relay #{inspect(relay)} of outbox #{inspect(name)} is not available: " <>
      "the module does not exist or does not implement Outboxer.Relay." <> hint
  end

  defp validate_backoff!(_name, fun) when is_function(fun, 1), do: :ok

  defp validate_backoff!(name, spec) do
    case {Keyword.get(spec, :min), Keyword.get(spec, :max)} do
      {min, max} when is_integer(min) and is_integer(max) and min >= 0 and max >= min ->
        :ok

      _other ->
        raise ArgumentError,
              "invalid :backoff for outbox #{inspect(name)}: expected [min: ms, max: ms] with " <>
                "0 <= min <= max, or a 1-arity function, got: #{inspect(spec)}"
    end
  end

  defp format(reason) when is_binary(reason), do: reason
  defp format(%{__exception__: true} = exception), do: Exception.message(exception)
  defp format(reason), do: inspect(reason)
end
