defmodule Outboxer.RelayTest do
  use ExUnit.Case, async: true

  alias Outboxer.Envelope
  alias Outboxer.Relay

  doctest Outboxer.Envelope

  defp envelope(id, group),
    do: %Envelope{id: id, outbox: :o, type: "t", payload: "", group_key: group}

  describe "sequential/2" do
    test "delivers in order and reports every result" do
      envelopes = [envelope(1, "a"), envelope(2, nil), envelope(3, "b")]
      test_pid = self()

      assert {:partial, %{1 => :ok, 2 => :ok, 3 => :ok}} =
               Relay.sequential(envelopes, fn envelope -> send(test_pid, envelope.id) && :ok end)

      assert_received 1
      assert_received 2
      assert_received 3
    end

    test "skips the rest of a group after a failure, and nothing else" do
      envelopes = [
        envelope(1, "a"),
        envelope(2, "a"),
        envelope(3, "b"),
        envelope(4, nil),
        envelope(5, "a")
      ]

      fun = fn envelope -> if envelope.id == 1, do: {:error, :nope}, else: :ok end

      assert {:partial, results} = Relay.sequential(envelopes, fun)
      assert results == %{1 => {:error, :nope}, 3 => :ok, 4 => :ok}
    end

    test "a failing ungrouped message blocks nothing" do
      envelopes = [envelope(1, nil), envelope(2, nil)]
      fun = fn envelope -> if envelope.id == 1, do: {:error, :nope}, else: :ok end
      assert {:partial, %{1 => {:error, :nope}, 2 => :ok}} = Relay.sequential(envelopes, fun)
    end

    test "treats {:ok, term} as success and anything else as an error" do
      assert {:partial, %{1 => :ok}} =
               Relay.sequential([envelope(1, nil)], fn _envelope -> {:ok, :sent} end)

      assert {:partial, %{1 => {:error, {:bad_return, :what}}}} =
               Relay.sequential([envelope(1, nil)], fn _envelope -> :what end)
    end

    test "turns raises, throws and exits into errors" do
      assert {:partial, %{1 => {:error, %RuntimeError{message: "boom"}}}} =
               Relay.sequential([envelope(1, nil)], fn _envelope -> raise "boom" end)

      assert {:partial, %{1 => {:error, {:throw, :ball}}}} =
               Relay.sequential([envelope(1, nil)], fn _envelope -> throw(:ball) end)

      assert {:partial, %{1 => {:error, {:exit, :bye}}}} =
               Relay.sequential([envelope(1, nil)], fn _envelope -> exit(:bye) end)
    end
  end

  describe "Outboxer.Relay.Function" do
    test "accepts a function or an MFA" do
      assert {:ok, fun} = Relay.Function.init(fun: fn _envelope -> :ok end)
      assert {:partial, %{1 => :ok}} = Relay.Function.deliver([envelope(1, nil)], fun)

      assert {:ok, fun} = Relay.Function.init(fun: {__MODULE__, :forward, [self()]})
      assert {:partial, %{7 => :ok}} = Relay.Function.deliver([envelope(7, nil)], fun)
      assert_received {:forwarded, 7}
    end

    test "rejects anything else" do
      assert {:error, message} = Relay.Function.init([])
      assert message =~ ":fun"
      assert {:error, _message} = Relay.Function.init(fun: fn -> :ok end)
    end
  end

  describe "Outboxer.Backoff" do
    test "grows exponentially between min and max" do
      spec = [min: 100, max: 1_000]
      assert Outboxer.Backoff.delay(spec, 1) in 100..110
      assert Outboxer.Backoff.delay(spec, 2) in 200..220
      assert Outboxer.Backoff.delay(spec, 3) in 400..440
      assert Outboxer.Backoff.delay(spec, 5) == 1_000
      assert Outboxer.Backoff.delay(spec, 500) == 1_000
      assert Outboxer.Backoff.delay([min: 0, max: 0], 3) == 0
    end

    test "accepts a function" do
      assert Outboxer.Backoff.delay(fn attempt -> attempt * 10 end, 4) == 40
      assert Outboxer.Backoff.delay(fn _attempt -> -5 end, 1) == 0
    end
  end

  describe "Outboxer.Message" do
    test "use provides defaults" do
      event = %Outboxer.Test.Event{n: 1, group: "g"}
      assert Outboxer.Test.Event.outbox() == nil
      assert Outboxer.Test.Event.type() == "Outboxer.Test.Event"
      assert Outboxer.Test.Event.headers(event) == %{}
      assert JSON.decode!(Outboxer.Test.Event.encode(event)) == %{"n" => 1, "group" => "g"}
    end

    test "every default can be overridden" do
      custom = %Outboxer.Test.Custom{body: "b", trace: "t"}
      assert Outboxer.Test.Custom.outbox() == :custom
      assert Outboxer.Test.Custom.type() == "custom.v1"
      assert Outboxer.Test.Custom.encode(custom) == "raw:b"
      assert Outboxer.Test.Custom.group_key(custom) == nil
    end
  end

  def forward(envelope, pid) do
    send(pid, {:forwarded, envelope.id})
    :ok
  end
end
