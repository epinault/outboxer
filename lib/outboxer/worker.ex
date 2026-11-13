defmodule Outboxer.Worker do
  @moduledoc false
  # One process per outbox partition. It polls, hands batches to the
  # dispatcher, and sleeps when there is nothing to do.
  #
  # Polling is adaptive: a full batch means there is probably more, so the
  # worker goes again straight away; anything else means the partition is
  # drained (or owned by another node), so it waits `poll_interval` with a
  # little jitter. A `:wake` message cuts the wait short.

  use GenServer

  alias Outboxer.Config
  alias Outboxer.Dispatcher

  require Logger

  def start_link({name, outbox, partition}) do
    GenServer.start_link(__MODULE__, {name, outbox, partition},
      name: {:via, Registry, {Config.registry(name), {outbox, partition}}}
    )
  end

  def child_spec({_name, _outbox, partition} = arg) do
    %{id: {__MODULE__, partition}, start: {__MODULE__, :start_link, [arg]}}
  end

  @doc "Asks the worker of a partition (or of every partition) to poll now."
  def wake(name, outbox, partition \\ :all)

  def wake(name, outbox, :all) do
    conf = Config.fetch!(name)

    case conf.outboxes do
      %{^outbox => %{partitions: partitions}} ->
        Enum.each(0..(partitions - 1), &wake(name, outbox, &1))

      _other ->
        :ok
    end
  end

  def wake(name, outbox, partition) do
    registry = Config.registry(name)

    if Process.whereis(registry) do
      for {pid, _value} <- Registry.lookup(registry, {outbox, partition}), do: send(pid, :wake)
    end

    :ok
  end

  @impl GenServer
  def init({name, outbox, partition}) do
    {:ok, %{name: name, outbox: outbox, partition: partition, timer: nil}, {:continue, :poll}}
  end

  @impl GenServer
  def handle_continue(:poll, state), do: {:noreply, poll(state)}

  @impl GenServer
  def handle_info(:poll, state), do: {:noreply, poll(%{state | timer: nil})}

  def handle_info(:wake, state) do
    cancel(state.timer)
    flush_wakes()
    {:noreply, poll(%{state | timer: nil})}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp poll(state) do
    conf = Config.fetch!(state.name)
    outbox = Config.outbox!(conf, state.outbox)

    delay =
      case run(conf, outbox, state.partition) do
        {:ok, %{fetched: fetched}} when fetched >= outbox.batch_size -> 0
        _other -> jitter(outbox.poll_interval)
      end

    %{state | timer: Process.send_after(self(), :poll, delay)}
  end

  # A database hiccup must not take the worker (and, through restart
  # intensity, the whole tree) down. Log it and try again on the next poll.
  defp run(conf, outbox, partition) do
    Dispatcher.run(conf, outbox, partition)
  rescue
    exception ->
      Logger.error(
        "Outboxer worker #{inspect(outbox.name)}/#{partition} failed: " <>
          Exception.format(:error, exception, __STACKTRACE__)
      )

      :error
  end

  defp jitter(interval), do: interval + :rand.uniform(div(interval, 10) + 1) - 1

  defp cancel(nil), do: :ok
  defp cancel(timer), do: Process.cancel_timer(timer)

  defp flush_wakes do
    receive do
      :wake -> flush_wakes()
      :poll -> flush_wakes()
    after
      0 -> :ok
    end
  end
end
