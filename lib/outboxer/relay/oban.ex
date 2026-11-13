if Code.ensure_loaded?(Oban) do
  defmodule Outboxer.Relay.Oban do
    @moduledoc """
    Relays messages into [Oban](https://hex.pm/packages/oban) as jobs.

    This module only exists when `:oban` is in your dependencies.

        {Outboxer,
         repo: MyApp.Repo,
         outboxes: [
           emails: [relay: {Outboxer.Relay.Oban, worker: MyApp.Workers.SendEmail, job_opts: [queue: :mailers]}]
         ]}

    ## Options

    #{NimbleOptions.docs(Outboxer.Relay.Oban.Options.schema())}

    ## Job args

    Unless you pass `:args`, a job gets:

        %{
          "id" => "emails-42",            # Outboxer.Envelope.dedup_id/1
          "type" => "MyApp.Events.Welcome",
          "group_key" => "user:7",        # or nil
          "headers" => %{},
          "payload" => %{"user_id" => 7}  # see the :payload option
        }

    ## Do you need an outbox in front of Oban?

    Often you do not. `Oban.insert/2` inside your own transaction is already
    transactional when Oban uses the same repo, and it is one moving part
    fewer. Reach for this relay when:

      * Oban lives in a **different database** than the business data, so the
        job insert cannot join your transaction.
      * You want jobs enqueued **in order** per group (see below).
      * Your code publishes domain messages and should not know which of them
        end up as jobs, in Kafka, or both.

    ## Guarantees

    When Outboxer and Oban use the same Postgres repo, the job insert joins
    the batch transaction: jobs are inserted and outbox rows removed
    atomically, so a message becomes exactly one job. In every other setup
    (different repos, SQLite) the handoff is at-least-once and a job may be
    inserted twice; add an Oban `unique` constraint on the `"id"` arg through
    `:job_opts` if that matters.

    ## Ordering

    Outboxer inserts jobs in order, so job ids follow message order within a
    group. Oban executes them in that order only when the queue runs one job
    at a time (`queues: [mailers: 1]`) on a single node, or with Oban Pro's
    partitioned queues. With ordinary concurrency, jobs run in parallel.
    """

    @behaviour Outboxer.Relay

    alias Outboxer.Envelope
    alias Outboxer.Relay.Oban.Options

    @impl Outboxer.Relay
    def init(opts) do
      with {:ok, opts} <- NimbleOptions.validate(opts, Options.schema()) do
        {:ok, Map.new(opts)}
      end
    end

    @impl Outboxer.Relay
    def deliver(envelopes, state) do
      changesets = Enum.map(envelopes, &changeset(&1, state))

      case Enum.find(changesets, &(not &1.valid?)) do
        nil ->
          Oban.insert_all(state.oban, changesets)
          :ok

        invalid ->
          {:error, "invalid Oban job: #{inspect(invalid.errors)}"}
      end
    end

    defp changeset(%Envelope{} = envelope, state) do
      worker = if is_function(state.worker, 1), do: state.worker.(envelope), else: state.worker

      job_opts =
        if is_function(state.job_opts, 1), do: state.job_opts.(envelope), else: state.job_opts

      worker.new(args(envelope, state), job_opts)
    end

    defp args(envelope, %{args: fun}) when is_function(fun, 1), do: fun.(envelope)

    defp args(%Envelope{} = envelope, %{payload: payload}) do
      %{
        "id" => Envelope.dedup_id(envelope),
        "type" => envelope.type,
        "group_key" => envelope.group_key,
        "headers" => envelope.headers,
        "payload" => payload(envelope.payload, payload)
      }
    end

    defp payload(binary, :json), do: JSON.decode!(binary)
    defp payload(binary, :raw), do: binary
    defp payload(binary, :base64), do: Base.encode64(binary)
  end
end
