defmodule Outboxer.Pruner do
  @moduledoc false
  # Deletes delivered messages once their retention has passed. Only started
  # when at least one outbox uses `retention: {:keep, seconds}`.
  #
  # Deletes go in bounded batches so that a large backlog of delivered rows
  # never turns into one long-running statement.

  use GenServer

  alias Outboxer.Config

  require Logger

  @batch 1_000

  def start_link(name), do: GenServer.start_link(__MODULE__, name)

  @doc "Runs one pruning pass for every outbox that keeps delivered messages."
  @spec prune(Config.t()) :: non_neg_integer()
  def prune(%Config{} = conf) do
    now = DateTime.utc_now()

    conf.outboxes
    |> Map.values()
    |> Enum.reduce(0, fn
      %{retention: {:keep, seconds}, name: outbox}, total ->
        count = prune_outbox(conf, outbox, DateTime.add(now, -seconds, :second), 0)

        :telemetry.execute([:outboxer, :prune], %{count: count}, %{
          name: conf.name,
          outbox: outbox
        })

        total + count

      _delete_on_delivery, total ->
        total
    end)
  end

  @impl GenServer
  def init(name) do
    schedule(Config.fetch!(name))
    {:ok, name}
  end

  @impl GenServer
  def handle_info(:prune, name) do
    conf = Config.fetch!(name)

    try do
      prune(conf)
    rescue
      exception ->
        Logger.error(
          "Outboxer pruner failed: " <> Exception.format(:error, exception, __STACKTRACE__)
        )
    end

    schedule(conf)
    {:noreply, name}
  end

  defp prune_outbox(conf, outbox, cutoff, total) do
    case conf.storage.prune(conf, outbox, cutoff, @batch) do
      @batch -> prune_outbox(conf, outbox, cutoff, total + @batch)
      count -> total + count
    end
  end

  defp schedule(conf), do: Process.send_after(self(), :prune, conf.prune_interval)
end
