defmodule Outboxer.Relay do
  @moduledoc """
  The behaviour for a relay: the thing that takes messages out of the outbox
  and hands them to a broker, a queue or anything else.

  Outboxer ships relays for Kafka (`Outboxer.Relay.Kafka`), SQS
  (`Outboxer.Relay.SQS`), Oban (`Outboxer.Relay.Oban`) and plain functions
  (`Outboxer.Relay.Function`). Each of the first three is compiled only when
  its dependency is in your project. Writing your own is a matter of
  implementing this behaviour; see the "Custom relays" guide.

  ## Lifecycle

    1. `c:init/1` runs once when the Outboxer supervisor starts. Validate the
       options and return the state that later calls receive. Raise or return
       `{:error, reason}` to refuse to start with a bad configuration.
    2. `c:children/1` (optional) returns child specs to supervise next to the
       outbox workers, for example a broker client.
    3. `c:deliver/2` is called with a batch of envelopes, in delivery order.

  ## What `deliver/2` returns

    * `:ok` - every message in the batch was delivered.
    * `{:error, reason}` - nothing was delivered.
    * `{:partial, results}` - a map of envelope id to `:ok` or
      `{:error, reason}`. Ids left out of the map count as "not attempted"
      and stay in the outbox untouched.

  ## The ordering rule

  For ordering to hold, a relay must not deliver a message after an earlier
  message **of the same group** has failed. Leave it out of the results (or
  report an error for it). `sequential/2` implements this rule for relays that
  send one message at a time.

  An exception raised from `deliver/2` is treated as `{:error, exception}` for
  the whole batch.
  """

  alias Outboxer.Envelope

  @type state :: term()
  @type result :: :ok | {:error, term()}
  @type deliver_result ::
          :ok | {:error, term()} | {:partial, %{optional(pos_integer()) => result()}}

  @doc "Validates the relay options and builds the relay state."
  @callback init(opts :: keyword()) :: {:ok, state()} | {:error, term()}

  @doc "Child specs to supervise alongside the outbox workers."
  @callback children(state()) :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc "Delivers a batch of envelopes, given in delivery order."
  @callback deliver(envelopes :: [Envelope.t(), ...], state()) :: deliver_result()

  @optional_callbacks children: 1

  @doc """
  Delivers envelopes one by one with `fun`, honouring the ordering rule.

  `fun` receives an envelope and returns `:ok` or `{:error, reason}`. Once a
  message of a group fails, later messages of that group are skipped. Messages
  without a group never block anything.
  """
  @spec sequential([Envelope.t()], (Envelope.t() -> result())) :: deliver_result()
  def sequential(envelopes, fun) when is_function(fun, 1) do
    {results, _failed_groups} =
      Enum.reduce(envelopes, {%{}, MapSet.new()}, fn envelope, {results, failed} ->
        if envelope.group_key != nil and MapSet.member?(failed, envelope.group_key) do
          {results, failed}
        else
          deliver_one(envelope, fun, results, failed)
        end
      end)

    {:partial, results}
  end

  defp deliver_one(envelope, fun, results, failed) do
    case safe_call(fun, envelope) do
      :ok -> {Map.put(results, envelope.id, :ok), failed}
      {:error, _reason} = error -> {Map.put(results, envelope.id, error), block(failed, envelope)}
    end
  end

  defp block(failed, %Envelope{group_key: nil}), do: failed
  defp block(failed, %Envelope{group_key: key}), do: MapSet.put(failed, key)

  defp safe_call(fun, envelope) do
    case fun.(envelope) do
      :ok -> :ok
      {:ok, _value} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:bad_return, other}}
    end
  rescue
    exception -> {:error, exception}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
