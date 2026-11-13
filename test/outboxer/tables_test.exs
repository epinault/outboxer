defmodule Outboxer.TablesTest do
  # Outboxes of one instance living in different tables.
  use Outboxer.Case, async: false, parameterize: Outboxer.Case.repos()

  alias Outboxer.Storage.Record
  alias Outboxer.Test.Custom
  alias Outboxer.Test.Order

  @fast [poll_interval: 20, backoff: [min: 20, max: 40]]
  @orders [table: "order_outbox"] ++ @fast

  defp count(context, table), do: context.repo.aggregate(table, :count)

  defp counts(name, outbox), do: Outboxer.counts(outbox, name: name)

  test "each outbox writes to its own table, and one without :table to the instance table",
       context do
    name = start_outboxer(context, [orders: @orders, audit: @fast], testing: :manual)

    {:ok, _result} =
      context.repo.transaction(fn ->
        Outboxer.publish(for(n <- 1..3, do: %Event{n: n}), name: name, outbox: :orders)
        Outboxer.publish(%Event{n: 9}, name: name, outbox: :audit)
      end)

    assert count(context, "order_outbox") == 3
    assert count(context, "outboxer_messages") == 1
    assert counts(name, :orders).pending == 3
    assert counts(name, :audit).pending == 1
  end

  test "one publish call spanning two tables returns envelopes in the caller's order", context do
    name = start_outboxer(context, [orders: @orders, custom: @fast], testing: :manual)

    messages = [
      %Order{n: 1},
      %Custom{body: "a", trace: "t"},
      %Order{n: 2},
      %Custom{body: "b", trace: "t"}
    ]

    {:ok, envelopes} = Outboxer.publish(messages, name: name)

    assert Enum.map(envelopes, &{&1.outbox, &1.payload}) == [
             {:orders, ~s({"n":1})},
             {:custom, "raw:a"},
             {:orders, ~s({"n":2})},
             {:custom, "raw:b"}
           ]

    # Each envelope carries the id of its own row.
    for envelope <- envelopes do
      pending = Outboxer.list_pending(envelope.outbox, name: name)
      assert Enum.find(pending, &(&1.id == envelope.id)).payload == envelope.payload
    end

    assert count(context, "order_outbox") == 2
    assert count(context, "outboxer_messages") == 2
  end

  test "both tables are delivered, each in order", context do
    name = start_outboxer(context, orders: @orders, audit: @fast)

    Outboxer.publish(for(n <- 1..5, do: %Event{n: n, group: "g"}), name: name, outbox: :orders)
    Outboxer.publish(for(n <- 6..10, do: %Event{n: n, group: "g"}), name: name, outbox: :audit)

    delivered = collect(10)
    assert for(e <- delivered, e.outbox == :orders, do: n(e)) == Enum.to_list(1..5)
    assert for(e <- delivered, e.outbox == :audit, do: n(e)) == Enum.to_list(6..10)

    eventually(fn ->
      count(context, "order_outbox") + count(context, "outboxer_messages") == 0
    end)
  end

  test "the same id in two tables: settling one outbox never touches the other", context do
    name = start_outboxer(context, [orders: @orders, audit: @fast], testing: :manual)
    {:ok, order} = Outboxer.publish(%Event{n: 1}, name: name, outbox: :orders)
    now = DateTime.utc_now()

    # A row of another outbox, in another table, with the very same id.
    context.repo.insert_all({"outboxer_messages", Record}, [
      %{
        id: order.id,
        outbox: "audit",
        partition: 0,
        type: Event.type(),
        payload: ~s({"n":2}),
        status: "pending",
        attempts: 0,
        scheduled_at: now,
        inserted_at: now
      }
    ])

    assert %{delivered: 1} = Outboxer.Testing.drain(:orders, name: name)
    assert [delivered] = collect(1)
    assert {delivered.outbox, n(delivered)} == {:orders, 1}
    assert count(context, "order_outbox") == 0
    assert [%{id: id}] = Outboxer.list_pending(:audit, name: name)
    assert id == order.id

    # Discarding by id is scoped to the outbox too.
    assert Outboxer.discard(:orders, [order.id], name: name) == 0
    assert counts(name, :audit).pending == 1
    assert Outboxer.discard(:audit, [order.id], name: name) == 1
    assert counts(name, :audit).pending == 0
  end

  test "retry, dead, retry_dead and discard work in a per-outbox table", context do
    failures = start_failures(fn _envelope -> true end)
    name = start_outboxer(context, orders: [fail: failures, max_attempts: 2] ++ @orders)
    Outboxer.publish([%Event{n: 1}, %Event{n: 2}], name: name, outbox: :orders)

    eventually(fn -> counts(name, :orders).dead == 2 end)
    assert count(context, "order_outbox") == 2
    assert count(context, "outboxer_messages") == 0

    [first, second] = Outboxer.list_dead(:orders, name: name)
    assert first.attempts == 2
    assert Outboxer.discard(:orders, [first.id], name: name) == 1

    set_failures(failures, fn _envelope -> false end)
    assert Outboxer.retry_dead(:orders, name: name, ids: [second.id]) == 1
    assert [envelope] = collect(1)
    assert n(envelope) == 2
    eventually(fn -> count(context, "order_outbox") == 0 end)
  end

  test "retention and pruning apply per table", context do
    name =
      start_outboxer(
        context,
        [orders: [retention: {:keep, 1}] ++ @orders, audit: @fast],
        prune_interval: 50
      )

    Outboxer.publish(%Event{n: 1}, name: name, outbox: :orders)
    Outboxer.publish(%Event{n: 2}, name: name, outbox: :audit)
    collect(2)

    eventually(fn -> counts(name, :orders).delivered == 1 end)
    eventually(fn -> count(context, "outboxer_messages") == 0 end)
    eventually(fn -> count(context, "order_outbox") == 0 end)
  end

  test "unknown outbox is rejected by discard", context do
    name = start_outboxer(context, [orders: @orders], testing: :manual)

    assert_raise ArgumentError, ~r/unknown outbox :nope/, fn ->
      Outboxer.discard(:nope, [1], name: name)
    end
  end
end
