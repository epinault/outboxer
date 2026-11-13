defmodule Outboxer.Relay.ObanTest do
  use Outboxer.Case, async: false

  import Ecto.Query

  alias Outboxer.Test.JobWorker
  alias Outboxer.Test.PgRepo

  @moduletag :postgres

  setup do
    PgRepo.delete_all("outboxer_messages")
    PgRepo.delete_all("oban_jobs")

    start_supervised!(
      {Oban, name: Oban, repo: PgRepo, testing: :manual, queues: false, plugins: false}
    )

    {:ok, repo: PgRepo}
  end

  defp jobs, do: PgRepo.all(from(j in Oban.Job, order_by: j.id))

  defp start(context, relay_opts, outbox_opts \\ []) do
    relay = {Outboxer.Relay.Oban, Keyword.merge([worker: JobWorker], relay_opts)}
    start_outboxer(context, [jobs: [relay: relay] ++ outbox_opts], testing: :manual)
  end

  test "init validates options" do
    assert {:error, %NimbleOptions.ValidationError{}} = Outboxer.Relay.Oban.init([])
    assert {:ok, %{oban: Oban, payload: :json}} = Outboxer.Relay.Oban.init(worker: JobWorker)
  end

  test "turns messages into jobs, in order, and empties the outbox", context do
    name = start(context, [])
    Outboxer.publish(for(n <- 1..3, do: %Event{n: n, group: "g"}), name: name, outbox: :jobs)

    assert %{delivered: 3} = Outboxer.Testing.drain(:jobs, name: name)
    assert %{pending: 0} = Outboxer.counts(:jobs, name: name)

    assert [first, _second, _third] = jobs = jobs()
    assert Enum.map(jobs, & &1.args["payload"]["n"]) == [1, 2, 3]
    assert first.worker == "Outboxer.Test.JobWorker"
    assert first.queue == "outbox"

    assert %{
             "id" => "jobs-" <> _id,
             "type" => "Outboxer.Test.Event",
             "group_key" => "g",
             "headers" => %{}
           } =
             first.args
  end

  test "job_opts, args and worker can be static or computed per message", context do
    name =
      start(context,
        worker: fn _envelope -> JobWorker end,
        args: fn envelope -> %{"n" => n(envelope)} end,
        job_opts: fn envelope -> [queue: :special, priority: n(envelope)] end
      )

    Outboxer.publish(%Event{n: 2}, name: name, outbox: :jobs)
    Outboxer.Testing.drain(:jobs, name: name)

    assert [%{args: %{"n" => 2}, queue: "special", priority: 2}] = jobs()
  end

  test "payload: :raw and :base64 pass the payload through", context do
    name = start(context, payload: :base64)
    Outboxer.publish(%Event{n: 1}, name: name, outbox: :jobs)
    Outboxer.Testing.drain(:jobs, name: name)

    assert [%{args: %{"payload" => encoded}}] = jobs()
    assert encoded |> Base.decode64!() |> JSON.decode!() == %{"n" => 1, "group" => nil}
  end

  test "the handoff is atomic: if the batch transaction fails, no job is left behind", context do
    name = start(context, [])
    Outboxer.publish(%Event{n: 1}, name: name, outbox: :jobs)

    # Run the batch inside a transaction that is then rolled back, as a crash
    # between "jobs inserted" and "commit" would.
    PgRepo.transaction(fn ->
      assert %{delivered: 1} = Outboxer.Testing.drain(:jobs, name: name)
      assert length(jobs()) == 1
      PgRepo.rollback(:crash)
    end)

    assert jobs() == []
    assert %{pending: 1} = Outboxer.counts(:jobs, name: name)
  end

  test "a payload that is not JSON fails the batch and is retried", context do
    name = start(context, [], backoff: [min: 0, max: 0])

    ExUnit.CaptureLog.capture_log(fn ->
      Outboxer.publish(%Outboxer.Test.Custom{body: "not json", trace: "t"},
        name: name,
        outbox: :jobs
      )

      assert %{delivered: 0, failed: failed} = Outboxer.Testing.drain(:jobs, name: name)
      assert failed > 0
    end)

    assert jobs() == []
  end
end
