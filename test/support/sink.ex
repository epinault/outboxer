defmodule Outboxer.Test.Sink do
  @moduledoc false
  # A relay for tests. It forwards every envelope to a test process and lets
  # the test decide, per message, whether delivery succeeds.
  #
  # Options:
  #   * :pid  - process that receives `{:delivered, envelope}`
  #   * :fail - an `Agent` holding a function `envelope -> boolean`; when it
  #             returns true the delivery of that envelope fails
  #   * :mode - :sequential (default) or :batch (all-or-nothing)

  @behaviour Outboxer.Relay

  @impl Outboxer.Relay
  def init(opts), do: {:ok, Map.new(opts)}

  @impl Outboxer.Relay
  def deliver(envelopes, %{mode: :batch} = state) do
    if Enum.any?(envelopes, &fail?(state, &1)) do
      {:error, :batch_failed}
    else
      Enum.each(envelopes, &send(state.pid, {:delivered, &1}))
      :ok
    end
  end

  def deliver(envelopes, state) do
    Outboxer.Relay.sequential(envelopes, fn envelope ->
      if fail?(state, envelope) do
        {:error, :boom}
      else
        send(state.pid, {:delivered, envelope})
        :ok
      end
    end)
  end

  defp fail?(%{fail: agent}, envelope), do: Agent.get(agent, & &1).(envelope)
  defp fail?(_state, _envelope), do: false
end
