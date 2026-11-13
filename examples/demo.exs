# A runnable tour of Outboxer on SQLite. No Docker, no broker:
#
#     mix run examples/demo.exs
#
# It creates a throwaway database, publishes "order" events inside
# transactions (one of which rolls back), and relays them through a function
# that fails now and then, so you can watch ordering and retries at work.

defmodule Demo.Repo do
  use Ecto.Repo, otp_app: :outboxer, adapter: Ecto.Adapters.SQLite3
end

defmodule Demo.Migration do
  use Ecto.Migration

  def up, do: Outboxer.Migration.up()
  def down, do: Outboxer.Migration.down()
end

defmodule Demo.OrderEvent do
  use Outboxer.Message, outbox: :orders, type: "order.event"

  defstruct [:order_id, :step]

  @impl true
  def group_key(%__MODULE__{order_id: id}), do: "order:#{id}"
end

defmodule Demo.Sink do
  # Stands in for a broker. Fails one delivery in four.
  def deliver(envelope) do
    if :rand.uniform(4) == 1 do
      IO.puts("  ✗ #{envelope.group_key} #{envelope.payload}  (simulated outage, will retry)")
      {:error, :broker_unavailable}
    else
      attempt = if envelope.attempts > 0, do: "  [after #{envelope.attempts} failed attempt(s)]", else: ""
      IO.puts("  ✓ #{envelope.group_key} #{envelope.payload}#{attempt}")
      :ok
    end
  end
end

defmodule Demo do
  def run do
    Logger.configure(level: :warning)
    database = Path.join(System.tmp_dir!(), "outboxer_demo_#{System.unique_integer([:positive])}.sqlite3")

    {:ok, _repo} = Demo.Repo.start_link(database: database, log: false)
    Ecto.Migrator.up(Demo.Repo, 1, Demo.Migration, log: false)

    # In an application this is one line in your supervision tree.
    {:ok, _outboxer} =
      Outboxer.start_link(
        repo: Demo.Repo,
        outboxes: [
          orders: [
            relay: {Outboxer.Relay.Function, fun: &Demo.Sink.deliver/1},
            partitions: 2,
            poll_interval: 100,
            backoff: [min: 100, max: 400]
          ]
        ]
      )

    IO.puts("\nPublishing 3 steps for each of 3 orders, each step in its own transaction...")

    for step <- ~w(placed paid shipped), order_id <- 1..3 do
      {:ok, _envelope} =
        Demo.Repo.transaction(fn ->
          # ... your business writes go here, in the same transaction ...
          {:ok, envelope} = Outboxer.publish(%Demo.OrderEvent{order_id: order_id, step: step})
          envelope
        end)
    end

    IO.puts("Publishing one more inside a transaction that rolls back (it must never be relayed)...\n")

    Demo.Repo.transaction(fn ->
      Outboxer.publish(%Demo.OrderEvent{order_id: 99, step: "never-happened"})
      Demo.Repo.rollback(:changed_my_mind)
    end)

    IO.puts("Relayed (each order's steps arrive in order: placed, paid, shipped):\n")

    wait_until_empty()

    IO.puts("\nOutbox is empty: #{inspect(Outboxer.counts(:orders))}")
    for suffix <- ["", "-wal", "-shm"], do: File.rm(database <> suffix)
  end

  defp wait_until_empty do
    case Outboxer.counts(:orders) do
      %{pending: 0} ->
        :ok

      _backlog ->
        Process.sleep(50)
        wait_until_empty()
    end
  end
end

Demo.run()
