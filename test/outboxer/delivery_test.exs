defmodule Outboxer.DeliveryTest do
  use Outboxer.Case, async: false, parameterize: Outboxer.Case.repos()

  @fast [poll_interval: 20, backoff: [min: 20, max: 40]]

  defp publish(name, messages), do: Outboxer.publish(messages, name: name, outbox: :events)

  describe "delivery" do
    test "relays published messages and empties the table", context do
      name = start_outboxer(context, events: @fast)
      publish(name, for(n <- 1..10, do: %Event{n: n, group: "g"}))

      assert 10 |> collect() |> Enum.map(&n/1) == Enum.to_list(1..10)

      eventually(fn ->
        Outboxer.counts(:events, name: name) == %{pending: 0, delivered: 0, dead: 0}
      end)
    end

    test "does not relay a rolled back message", context do
      name = start_outboxer(context, events: @fast)

      context.repo.transaction(fn ->
        publish(name, %Event{n: 1})
        context.repo.rollback(:nope)
      end)

      publish(name, %Event{n: 2})
      assert [envelope] = collect(1)
      assert n(envelope) == 2
      refute_receive {:delivered, _envelope}, 100
    end

    test "works through more messages than one batch holds", context do
      name = start_outboxer(context, events: [batch_size: 7] ++ @fast)
      publish(name, for(n <- 1..50, do: %Event{n: n, group: "g"}))

      assert 50 |> collect() |> Enum.map(&n/1) == Enum.to_list(1..50)
    end

    test "delivers a message published outside a transaction without waiting for the poll",
         context do
      name = start_outboxer(context, events: [poll_interval: 60_000])
      # Let the worker finish its first, empty poll and go to sleep for a minute.
      Process.sleep(100)

      publish(name, %Event{n: 1})
      assert [_envelope] = collect(1, 2_000)
    end

    test "emits batch telemetry", context do
      ref = :telemetry_test.attach_event_handlers(self(), [[:outboxer, :relay, :batch, :stop]])
      name = start_outboxer(context, events: @fast)
      publish(name, [%Event{n: 1}, %Event{n: 2}])
      collect(2)

      assert_receive {[:outboxer, :relay, :batch, :stop], ^ref, %{duration: _duration},
                      %{name: ^name, outbox: :events, partition: 0, delivered: delivered}}
                     when delivered > 0
    end
  end

  describe "ordering" do
    test "keeps the order of every group across partitions and concurrent publishers", context do
      name = start_outboxer(context, events: [partitions: 4, batch_size: 5] ++ @fast)
      groups = for g <- 1..8, do: "group-#{g}"

      # Every group has its own publisher process, so publishers interleave
      # while each group's messages are written in sequence.
      groups
      |> Task.async_stream(
        fn group -> for n <- 1..25, do: publish(name, %Event{n: n, group: group}) end,
        max_concurrency: 8,
        timeout: 30_000
      )
      |> Stream.run()

      delivered = collect(200, 15_000)

      for group <- groups do
        assert delivered |> Enum.filter(&(&1.group_key == group)) |> Enum.map(&n/1) ==
                 Enum.to_list(1..25)
      end
    end

    test "ordering: :total delivers the whole outbox in publish order", context do
      name = start_outboxer(context, events: [ordering: :total, batch_size: 3] ++ @fast)
      publish(name, for(n <- 1..20, do: %Event{n: n, group: "g#{rem(n, 3)}"}))

      assert 20 |> collect() |> Enum.map(&n/1) == Enum.to_list(1..20)
    end
  end

  describe "failures" do
    test "a failing message blocks its group but not the others", context do
      failures = start_failures(fn envelope -> envelope.group_key == "bad" end)
      name = start_outboxer(context, events: [fail: failures] ++ @fast)

      publish(name, [
        %Event{n: 1, group: "bad"},
        %Event{n: 2, group: "good"},
        %Event{n: 3, group: "bad"},
        %Event{n: 4, group: nil},
        %Event{n: 5, group: "good"}
      ])

      assert 3 |> collect() |> Enum.map(&n/1) == [2, 4, 5]
      refute_receive {:delivered, _envelope}, 200

      # Only the head of the blocked group is charged with attempts.
      assert [head, behind] = Outboxer.list_pending(:events, name: name)
      assert n(head) == 1 and head.attempts > 0 and head.last_error == ":boom"
      assert n(behind) == 3 and behind.attempts == 0

      # Once the cause is fixed the group resumes, in order.
      set_failures(failures, fn _envelope -> false end)
      assert 2 |> collect() |> Enum.map(&n/1) == [1, 3]
    end

    test "a failing ungrouped message does not block other ungrouped messages", context do
      failures = start_failures(fn envelope -> n(envelope) == 1 end)
      name = start_outboxer(context, events: [fail: failures] ++ @fast)
      publish(name, for(n <- 1..4, do: %Event{n: n}))

      assert 3 |> collect() |> Enum.map(&n/1) == [2, 3, 4]
    end

    test "retries with backoff until the message goes through", context do
      counter = :counters.new(1, [])

      failures =
        start_failures(fn _envelope ->
          :counters.add(counter, 1, 1)
          :counters.get(counter, 1) <= 3
        end)

      ref = :telemetry_test.attach_event_handlers(self(), [[:outboxer, :message, :retry]])
      name = start_outboxer(context, events: [fail: failures] ++ @fast)
      publish(name, %Event{n: 1, group: "g"})

      assert [envelope] = collect(1)
      assert envelope.attempts == 3

      assert_receive {[:outboxer, :message, :retry], ^ref, %{attempt: 1},
                      %{outbox: :events, error: ":boom"}}

      assert_receive {[:outboxer, :message, :retry], ^ref, %{attempt: 3}, _metadata}
    end

    test "with max_attempts the message dies and the group continues", context do
      failures = start_failures(fn envelope -> n(envelope) == 1 end)
      ref = :telemetry_test.attach_event_handlers(self(), [[:outboxer, :message, :dead]])
      name = start_outboxer(context, events: [fail: failures, max_attempts: 3] ++ @fast)
      publish(name, for(n <- 1..3, do: %Event{n: n, group: "g"}))

      assert 2 |> collect() |> Enum.map(&n/1) == [2, 3]
      assert_receive {[:outboxer, :message, :dead], ^ref, %{attempt: 3}, %{outbox: :events}}

      eventually(fn -> Outboxer.counts(:events, name: name).dead == 1 end)
      assert [dead] = Outboxer.list_dead(:events, name: name)
      assert n(dead) == 1 and dead.attempts == 3 and dead.last_error == ":boom"

      eventually(fn ->
        Outboxer.counts(:events, name: name) == %{pending: 0, delivered: 0, dead: 1}
      end)
    end

    test "dead messages can be retried or discarded", context do
      failures = start_failures(fn _envelope -> true end)
      name = start_outboxer(context, events: [fail: failures, max_attempts: 1] ++ @fast)
      publish(name, [%Event{n: 1}, %Event{n: 2}])
      eventually(fn -> Outboxer.counts(:events, name: name).dead == 2 end)

      [first, second] = Outboxer.list_dead(:events, name: name)
      assert Outboxer.discard(:events, [first.id], name: name) == 1

      set_failures(failures, fn _envelope -> false end)
      assert Outboxer.retry_dead(:events, name: name, ids: [second.id]) == 1
      assert [envelope] = collect(1)
      assert n(envelope) == 2

      eventually(fn ->
        Outboxer.counts(:events, name: name) == %{pending: 0, delivered: 0, dead: 0}
      end)
    end

    test "discarding a poison message unblocks its group", context do
      failures = start_failures(fn envelope -> n(envelope) == 1 end)
      name = start_outboxer(context, events: [fail: failures] ++ @fast)
      publish(name, [%Event{n: 1, group: "g"}, %Event{n: 2, group: "g"}])

      [poison | _rest] =
        eventually(fn ->
          match?([%{attempts: a} | _rest] when a > 0, pending(name)) && pending(name)
        end)

      refute_receive {:delivered, _envelope}, 50
      Outboxer.discard(:events, [poison.id], name: name)

      assert [envelope] = collect(1)
      assert n(envelope) == 2
    end

    test "an all-or-nothing relay failure keeps everything and retries", context do
      failures = start_failures(fn envelope -> n(envelope) == 3 end)
      name = start_outboxer(context, events: [fail: failures, mode: :batch] ++ @fast)
      publish(name, for(n <- 1..4, do: %Event{n: n, group: "g"}))

      refute_receive {:delivered, _envelope}, 150
      assert %{pending: 4} = Outboxer.counts(:events, name: name)

      set_failures(failures, fn _envelope -> false end)
      assert 4 |> collect() |> Enum.map(&n/1) == [1, 2, 3, 4]
    end

    test "a relay that raises is treated as a failed batch", context do
      relay = {Outboxer.Relay.Function, fun: {__MODULE__, :raising_sink, [self()]}}
      name = start_outboxer(context, events: [relay: relay] ++ @fast)

      ExUnit.CaptureLog.capture_log(fn ->
        publish(name, %Event{n: 1, group: "g"})
        assert_receive {:attempted, 1}
        assert_receive {:attempted, 1}
      end)

      assert [%{attempts: attempts, last_error: "kaboom"}] =
               Outboxer.list_pending(:events, name: name)

      assert attempts > 0
    end
  end

  describe "retention" do
    test "{:keep, seconds} marks messages delivered and the pruner removes them later", context do
      name =
        start_outboxer(context, [events: [retention: {:keep, 1}] ++ @fast], prune_interval: 50)

      publish(name, [%Event{n: 1}, %Event{n: 2}])
      collect(2)

      eventually(fn ->
        Outboxer.counts(:events, name: name) == %{pending: 0, delivered: 2, dead: 0}
      end)

      refute_receive {:delivered, _envelope}, 100
      eventually(fn -> Outboxer.counts(:events, name: name).delivered == 0 end)
    end

    test "ids are never reused after delete-on-delivery", context do
      name = start_outboxer(context, events: @fast)
      {:ok, first} = publish(name, %Event{n: 1})
      collect(1)
      eventually(fn -> Outboxer.counts(:events, name: name).pending == 0 end)

      {:ok, second} = publish(name, %Event{n: 2})
      assert second.id > first.id
    end
  end

  def raising_sink(envelope, pid) do
    send(pid, {:attempted, envelope.id |> is_integer() |> then(fn true -> 1 end)})
    raise "kaboom"
  end

  defp pending(name), do: Outboxer.list_pending(:events, name: name)
end
