if Code.ensure_loaded?(Postgrex) do
  defmodule Outboxer.Notifier do
    @moduledoc false
    # Listens on the Postgres "outboxer" channel and wakes the local worker of
    # the partition that received new messages. Started only with
    # `notify: true`. Polling stays on as the safety net, so a missed
    # notification costs latency, never a message.

    use GenServer

    alias Outboxer.Config
    alias Outboxer.Storage.Postgres

    @drop [
      :name,
      :pool,
      :pool_size,
      :pool_count,
      :telemetry_prefix,
      :log,
      :stacktrace,
      :queue_target,
      :queue_interval
    ]

    def start_link(name), do: GenServer.start_link(__MODULE__, name)

    @impl GenServer
    def init(name) do
      conf = Config.fetch!(name)
      opts = conf.repo.config() |> Keyword.drop(@drop) |> Keyword.put(:auto_reconnect, true)

      {:ok, listener} = Postgrex.Notifications.start_link(opts)
      # With auto_reconnect the subscription is restored after a reconnect.
      {:eventually, _ref} = Postgrex.Notifications.listen(listener, Postgres.channel())

      {:ok, %{name: name, prefix: "#{name}|"}}
    end

    @impl GenServer
    def handle_info({:notification, _pid, _ref, _channel, payload}, state) do
      with [name, outbox, partition] <- String.split(payload, "|"),
           true <- name == Atom.to_string(state.name),
           {partition, ""} <- Integer.parse(partition),
           {:ok, outbox} <- find_outbox(state.name, outbox) do
        Outboxer.Worker.wake(state.name, outbox, partition)
      end

      {:noreply, state}
    end

    def handle_info(_other, state), do: {:noreply, state}

    # Match against the configured names rather than creating atoms from
    # whatever arrives on the channel.
    defp find_outbox(name, outbox) do
      names = name |> Config.fetch!() |> Map.fetch!(:outboxes) |> Map.keys()

      case Enum.find(names, &(Atom.to_string(&1) == outbox)) do
        nil -> :error
        found -> {:ok, found}
      end
    end
  end
end
