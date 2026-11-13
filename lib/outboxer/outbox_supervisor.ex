defmodule Outboxer.OutboxSupervisor do
  @moduledoc false
  # Supervises everything one outbox needs: whatever processes its relay asks
  # for (a broker client, say), then one worker per partition.
  #
  # `:rest_for_one` because the workers depend on the relay's processes: if the
  # broker client restarts, the workers restart after it.

  use Supervisor

  alias Outboxer.Config.Outbox

  def start_link({name, %Outbox{} = outbox}) do
    Supervisor.start_link(__MODULE__, {name, outbox})
  end

  def child_spec({_name, %Outbox{name: outbox}} = arg) do
    %{id: {__MODULE__, outbox}, start: {__MODULE__, :start_link, [arg]}, type: :supervisor}
  end

  @impl Supervisor
  def init({name, %Outbox{} = outbox}) do
    workers =
      for partition <- 0..(outbox.partitions - 1),
          do: {Outboxer.Worker, {name, outbox.name, partition}}

    Supervisor.init(relay_children(outbox) ++ workers, strategy: :rest_for_one)
  end

  defp relay_children(%Outbox{relay: relay, relay_state: state}) do
    if function_exported?(relay, :children, 1), do: relay.children(state), else: []
  end
end
