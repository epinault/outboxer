defmodule Outboxer.SupervisorTest do
  use Outboxer.Case, async: false

  alias Outboxer.Test.SqliteRepo

  defp child_ids(supervisor),
    do: for({id, _pid, _type, _modules} <- Supervisor.which_children(supervisor), do: id)

  defp outbox_supervisor(name, outbox) do
    {_id, pid, :supervisor, _modules} =
      name |> Supervisor.which_children() |> List.keyfind({Outboxer.OutboxSupervisor, outbox}, 0)

    pid
  end

  setup do
    {:ok, repo: SqliteRepo}
  end

  test "has no application callback: nothing runs until it is added to a tree" do
    assert Application.spec(:outboxer, :mod) in [nil, []]

    assert %{id: MyOutbox, type: :supervisor} =
             Outboxer.child_spec(name: MyOutbox, repo: SqliteRepo)

    assert %{id: Outboxer} = Outboxer.child_spec(repo: SqliteRepo)
  end

  test "starts a registry and one worker per partition, and nothing else", context do
    name = start_outboxer(context, a: [partitions: 3], b: [])

    assert Enum.sort(child_ids(name)) ==
             Enum.sort([
               Outboxer.Config.registry(name),
               {Outboxer.OutboxSupervisor, :a},
               {Outboxer.OutboxSupervisor, :b}
             ])

    assert Enum.sort(child_ids(outbox_supervisor(name, :a))) ==
             for(p <- 0..2, do: {Outboxer.Worker, p})

    assert child_ids(outbox_supervisor(name, :b)) == [{Outboxer.Worker, 0}]
  end

  test "starts the pruner only when an outbox keeps delivered messages", context do
    name = start_outboxer(context, a: [retention: {:keep, 60}])
    assert Outboxer.Pruner in child_ids(name)
  end

  test "starts the relay's children before the workers", context do
    relay =
      {Outboxer.Relay.Kafka,
       client: :sup_test_brod, topic: "t", endpoints: [], brod: Outboxer.Test.FakeBrod}

    name = start_outboxer(context, a: [relay: relay])

    assert [{:brod_client, :sup_test_brod}, {Outboxer.Worker, 0}] =
             name |> outbox_supervisor(:a) |> child_ids() |> Enum.reverse()

    assert is_pid(Process.whereis(:sup_test_brod))
  end

  test "testing: :manual starts no processes but still accepts messages", context do
    name = start_outboxer(context, [a: []], testing: :manual)

    assert child_ids(name) == []
    assert {:ok, _envelope} = Outboxer.publish(%Event{n: 1}, name: name, outbox: :a)
    SqliteRepo.delete_all("outboxer_messages")
  end

  test "notify is ignored on SQLite", context do
    name = start_outboxer(context, [a: []], notify: true)
    refute Outboxer.Notifier in child_ids(name)
  end

  test "refuses to start with invalid options" do
    Process.flag(:trap_exit, true)
    assert_raise NimbleOptions.ValidationError, fn -> Outboxer.start_link(outboxes: []) end
  end

  test "a crashing worker is restarted and delivery continues", context do
    name = start_outboxer(context, a: [poll_interval: 20])
    [{worker, _value}] = Registry.lookup(Outboxer.Config.registry(name), {:a, 0})

    Process.exit(worker, :kill)

    eventually(fn ->
      match?(
        [{pid, _value}] when pid != worker,
        Registry.lookup(Outboxer.Config.registry(name), {:a, 0})
      )
    end)

    Outboxer.publish(%Event{n: 1}, name: name, outbox: :a)
    assert [_envelope] = collect(1)
  end
end
