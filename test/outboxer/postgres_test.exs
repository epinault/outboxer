defmodule Outboxer.PostgresTest do
  # Behaviour that only exists on Postgres: several nodes sharing an outbox,
  # LISTEN/NOTIFY wake-up, write serialisation, and schema prefixes.
  use Outboxer.Case, async: false

  alias Outboxer.Test.PgRepo

  @moduletag :postgres
  @fast [poll_interval: 20, backoff: [min: 20, max: 40]]

  setup do
    PgRepo.delete_all("outboxer_messages")
    PgRepo.query!("DELETE FROM private.custom_outbox")
    {:ok, repo: PgRepo}
  end

  describe "several nodes on one outbox" do
    test "each message is delivered once and every group stays in order", context do
      outboxes = [events: [partitions: 3, batch_size: 4] ++ @fast]
      node_a = start_outboxer(context, outboxes)
      _node_b = start_outboxer(context, outboxes)
      _node_c = start_outboxer(context, outboxes)
      groups = for g <- 1..6, do: "group-#{g}"

      groups
      |> Task.async_stream(
        fn group ->
          for n <- 1..30,
              do: Outboxer.publish(%Event{n: n, group: group}, name: node_a, outbox: :events)
        end,
        max_concurrency: 6,
        timeout: 30_000
      )
      |> Stream.run()

      delivered = collect(180, 20_000)
      refute_receive {:delivered, _duplicate}, 200

      for group <- groups do
        assert delivered |> Enum.filter(&(&1.group_key == group)) |> Enum.map(&n/1) ==
                 Enum.to_list(1..30)
      end

      assert length(Enum.uniq_by(delivered, & &1.id)) == 180
    end

    test "a partition held by one node is reported busy to another", context do
      name = start_outboxer(context, [events: []], testing: :manual)
      conf = Outboxer.Config.fetch!(name)
      outbox = Outboxer.Config.outbox!(conf, :events)
      Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
      test_pid = self()

      holder =
        Task.async(fn ->
          conf.storage.with_partition(conf, :events, 0, fn ->
            send(test_pid, :locked)

            receive do
              :release -> :done
            end
          end)
        end)

      assert_receive :locked
      assert Outboxer.Dispatcher.run(conf, outbox, 0) == :busy

      send(holder.pid, :release)
      assert Task.await(holder) == {:ok, :done}
      assert {:ok, %{delivered: 1}} = Outboxer.Dispatcher.run(conf, outbox, 0)
    end

    test "a worker that dies mid-batch releases the partition and the batch is redelivered",
         context do
      name = start_outboxer(context, [events: []], testing: :manual)
      conf = Outboxer.Config.fetch!(name)
      outbox = Outboxer.Config.outbox!(conf, :events)
      Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
      test_pid = self()

      {pid, ref} =
        spawn_monitor(fn ->
          conf.storage.with_partition(conf, :events, 0, fn ->
            [_envelope] = conf.storage.fetch(conf, :events, 0, 10, DateTime.utc_now())
            send(test_pid, :fetched)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :fetched
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

      eventually(fn ->
        match?({:ok, %{delivered: 1}}, Outboxer.Dispatcher.run(conf, outbox, 0))
      end)

      assert [envelope] = collect(1)
      assert n(envelope) == 1
    end
  end

  describe "Outboxer.Testing.drain in manual mode" do
    test "does not depend on the partition lock, so sandboxed tests cannot starve each other",
         context do
      name = start_outboxer(context, [events: []], testing: :manual)
      conf = Outboxer.Config.fetch!(name)
      Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
      test_pid = self()

      # Another process holds the partition, as a concurrent test's sandbox
      # transaction would for as long as that test runs.
      holder =
        Task.async(fn ->
          conf.storage.with_partition(conf, :events, 0, fn ->
            send(test_pid, :locked)

            receive do
              :release -> :ok
            end
          end)
        end)

      assert_receive :locked
      assert %{delivered: 1} = Outboxer.Testing.drain(:events, name: name)
      send(holder.pid, :release)
      Task.await(holder)
    end
  end

  describe "notify" do
    test "a commit wakes the worker long before the next poll", context do
      name = start_outboxer(context, [events: [poll_interval: 60_000]], notify: true)
      # Let the worker finish its first poll and the listener subscribe.
      Process.sleep(300)

      {:ok, _result} =
        PgRepo.transaction(fn -> Outboxer.publish(%Event{n: 1}, name: name, outbox: :events) end)

      assert [_envelope] = collect(1, 2_000)
    end

    test "a publish-only node's notification wakes the relaying node", context do
      relay = start_outboxer(context, [events: [poll_interval: 60_000]], notify: true)
      Process.sleep(300)

      # Two instances in one VM cannot share a name, as two pods do. So the
      # publisher's notification is sent under the relay's name, which is what
      # a pod with the same configuration would send.
      start_supervised!(
        {Outboxer, name: NotifyPublisher, repo: PgRepo, relay: false, outboxes: [events: []]},
        id: NotifyPublisher
      )

      conf = %{Outboxer.Config.fetch!(NotifyPublisher) | name: relay}

      {:ok, _result} =
        PgRepo.transaction(fn ->
          {:ok, envelope} = Outboxer.publish(%Event{n: 1}, name: NotifyPublisher, outbox: :events)
          Outboxer.Storage.Postgres.notify(conf, :events, envelope.partition)
        end)

      assert [_envelope] = collect(1, 2_000)
    end

    test "nothing is delivered for a transaction that rolls back", context do
      name = start_outboxer(context, [events: [poll_interval: 60_000]], notify: true)
      Process.sleep(300)

      PgRepo.transaction(fn ->
        Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
        PgRepo.rollback(:nope)
      end)

      refute_receive {:delivered, _envelope}, 300
    end
  end

  describe "serialize_writes" do
    test "a second transaction publishing to the same group waits for the first", context do
      name = start_outboxer(context, [events: [serialize_writes: true]], testing: :manual)
      test_pid = self()

      first =
        Task.async(fn ->
          PgRepo.transaction(fn ->
            {:ok, envelope} =
              Outboxer.publish(%Event{n: 1, group: "g"}, name: name, outbox: :events)

            send(test_pid, :first_published)

            receive do
              :commit -> envelope.id
            end
          end)
        end)

      assert_receive :first_published

      second =
        Task.async(fn ->
          PgRepo.transaction(fn ->
            {:ok, envelope} =
              Outboxer.publish(%Event{n: 2, group: "g"}, name: name, outbox: :events)

            send(test_pid, :second_published)
            envelope.id
          end)
        end)

      # The second publisher is parked on the group lock, so it has not even
      # been given an id yet.
      refute_receive :second_published, 200
      send(first.pid, :commit)

      assert {:ok, first_id} = Task.await(first)
      assert {:ok, second_id} = Task.await(second)
      assert second_id > first_id
    end

    test "other groups are not held up", context do
      name = start_outboxer(context, [events: [serialize_writes: true]], testing: :manual)

      PgRepo.transaction(fn ->
        Outboxer.publish(%Event{n: 1, group: "a"}, name: name, outbox: :events)

        other =
          Task.async(fn ->
            PgRepo.transaction(fn ->
              Outboxer.publish(%Event{n: 2, group: "b"}, name: name, outbox: :events)
            end)
          end)

        assert {:ok, {:ok, _envelope}} = Task.await(other, 2_000)
      end)
    end
  end

  describe "table and prefix" do
    test "an instance can live in its own schema and table", context do
      name = start_outboxer(context, [events: @fast], prefix: "private", table: "custom_outbox")
      Outboxer.publish(for(n <- 1..3, do: %Event{n: n, group: "g"}), name: name, outbox: :events)

      assert 3 |> collect() |> Enum.map(&n/1) == [1, 2, 3]
      eventually(fn -> Outboxer.counts(:events, name: name).pending == 0 end)
      assert PgRepo.aggregate("outboxer_messages", :count) == 0
    end
  end
end
