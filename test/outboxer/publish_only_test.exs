defmodule Outboxer.PublishOnlyTest do
  # One instance that only publishes (a web pod) and another that relays (a
  # worker pod), sharing a database.
  use Outboxer.Case, async: false, parameterize: Outboxer.Case.repos()

  @fast [poll_interval: 20, backoff: [min: 20, max: 40]]

  # No relay is configured anywhere on the publishing side.
  defp start_publisher(context, outboxes, opts \\ []) do
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    name = Module.concat(Outboxer.TestPublisher, "N#{System.unique_integer([:positive])}")
    opts = Keyword.merge([name: name, repo: context.repo, relay: false, outboxes: outboxes], opts)
    start_supervised!({Outboxer, opts}, id: name)
    name
  end

  test "a publish-only instance starts nothing and needs no relay", context do
    publisher = start_publisher(context, events: [retention: {:keep, 60}])

    assert Supervisor.which_children(publisher) == []
    assert {:ok, envelope} = Outboxer.publish(%Event{n: 1}, name: publisher, outbox: :events)
    assert envelope.outbox == :events
    assert Outboxer.counts(:events, name: publisher).pending == 1
    refute_receive {:delivered, _envelope}, 100
  end

  test "messages published on one instance are delivered by another", context do
    outboxes = [events: [partitions: 2], orders: [table: "order_outbox"]]
    publisher = start_publisher(context, outboxes)

    context.repo.transaction(fn ->
      Outboxer.publish(for(n <- 1..6, do: %Event{n: n, group: "g#{rem(n, 3)}"}),
        name: publisher,
        outbox: :events
      )

      Outboxer.publish(%Event{n: 7}, name: publisher, outbox: :orders)
    end)

    refute_receive {:delivered, _envelope}, 100

    relay = start_outboxer(context, for({outbox, opts} <- outboxes, do: {outbox, opts ++ @fast}))
    delivered = collect(7)

    assert delivered |> Enum.map(&n/1) |> Enum.sort() == Enum.to_list(1..7)

    for group <- ["g0", "g1", "g2"] do
      ns = for e <- delivered, e.group_key == group, do: n(e)
      assert ns == Enum.sort(ns)
    end

    eventually(fn ->
      Outboxer.counts(:events, name: relay).pending +
        Outboxer.counts(:orders, name: relay).pending ==
        0
    end)
  end

  test "the admin functions work from the publish-only instance", context do
    failures = start_failures(fn _envelope -> true end)
    publisher = start_publisher(context, events: [])
    relay = start_outboxer(context, events: [fail: failures, max_attempts: 1] ++ @fast)

    Outboxer.publish([%Event{n: 1}, %Event{n: 2}], name: publisher, outbox: :events)
    eventually(fn -> Outboxer.counts(:events, name: publisher).dead == 2 end)

    [first, second] = Outboxer.list_dead(:events, name: publisher)
    assert Outboxer.discard(:events, [first.id], name: publisher) == 1

    set_failures(failures, fn _envelope -> false end)
    assert Outboxer.retry_dead(:events, name: publisher, ids: [second.id]) == 1
    assert [envelope] = collect(1)
    assert n(envelope) == 2

    eventually(fn ->
      Outboxer.counts(:events, name: relay) == %{pending: 0, delivered: 0, dead: 0}
    end)
  end

  test "drain refuses to run without a relay", context do
    publisher = start_publisher(context, events: [])

    assert_raise ArgumentError, ~r/started with relay: false/, fn ->
      Outboxer.Testing.drain(:events, name: publisher)
    end
  end

  test "a relaying instance refuses to start without a relay", context do
    assert_raise ArgumentError, ~r/outbox :events has no :relay/, fn ->
      Outboxer.start_link(name: NoRelayHere, repo: context.repo, outboxes: [events: []])
    end
  end
end
