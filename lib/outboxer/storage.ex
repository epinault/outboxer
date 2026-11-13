defmodule Outboxer.Storage do
  @moduledoc """
  The behaviour between Outboxer and the database that holds the outbox.

  Two adapters ship with the library, both built on Ecto:

    * `Outboxer.Storage.Postgres` - multi-node safe. Each delivery batch runs in
      a short transaction that holds a transaction-scoped advisory lock on the
      partition.
    * `Outboxer.Storage.SQLite` - single node. The one worker per partition is
      the only owner; delivery happens outside any transaction because SQLite
      has a single writer.

  The adapter is chosen from the repo's Ecto adapter, or set explicitly with
  the `:storage` option of `Outboxer`.

  ## Writing another adapter

  The contract an adapter has to honour:

    * `c:insert/2` must take part in the caller's database transaction. That is
      the whole point of the pattern: the message commits or rolls back with
      the business write. A store that cannot do this (Redis, for example) can
      still implement the behaviour, but what you get is a best-effort queue,
      not a transactional outbox.
    * `c:with_partition/4` must guarantee that at most one process in the whole
      cluster runs the function for a given outbox partition at a time, and
      must release that ownership if the process dies.
    * `c:fetch/5` must return ready messages in id order and must leave out
      every group that has a failed message waiting for its retry.
  """

  @typedoc """
  The instance configuration. An adapter can rely on these fields:

    * `:repo` - the `Ecto.Repo` module
    * `:table` - the outbox table name
    * `:prefix` - the schema of the table, or `nil`
    * `:name` - the Outboxer instance name
  """
  @type conf :: %{
          :repo => module(),
          :table => String.t(),
          :prefix => String.t() | nil,
          :name => atom(),
          optional(atom()) => term()
        }
  @type outbox :: atom()
  @type row :: %{
          outbox: outbox(),
          partition: non_neg_integer(),
          group_key: String.t() | nil,
          type: String.t(),
          payload: binary(),
          headers: map()
        }

  @doc "Inserts rows in the caller's transaction and returns their envelopes."
  @callback insert(conf(), [row()]) :: [Outboxer.Envelope.t()]

  @doc """
  Serialises concurrent publishers of the same group until their transaction ends.
  A no-op where the database already has a single writer.
  """
  @callback lock_group(conf(), outbox(), group_key :: String.t()) :: :ok

  @doc "Runs `fun` as the exclusive owner of a partition, or returns `:busy`."
  @callback with_partition(conf(), outbox(), partition :: non_neg_integer(), (-> result)) ::
              {:ok, result} | :busy
            when result: term()

  @doc "Returns up to `limit` deliverable messages of a partition, oldest first."
  @callback fetch(
              conf(),
              outbox(),
              partition :: non_neg_integer(),
              limit :: pos_integer(),
              now :: DateTime.t()
            ) ::
              [Outboxer.Envelope.t()]

  @doc "Records successful delivery: deletes the rows or marks them delivered."
  @callback ack(
              conf(),
              ids :: [pos_integer()],
              retention :: :delete | {:keep, pos_integer()},
              now :: DateTime.t()
            ) ::
              :ok

  @doc "Records a failed attempt and schedules the retry."
  @callback fail(conf(), ids :: [pos_integer()], retry_at :: DateTime.t(), error :: String.t()) ::
              :ok

  @doc "Moves messages to the dead state after their last allowed attempt."
  @callback kill(conf(), ids :: [pos_integer()], error :: String.t(), now :: DateTime.t()) :: :ok

  @doc "Deletes up to `limit` delivered messages older than `cutoff`. Returns how many."
  @callback prune(conf(), outbox(), cutoff :: DateTime.t(), limit :: pos_integer()) ::
              non_neg_integer()

  @doc "Tells other nodes that a partition has new messages. Optional."
  @callback notify(conf(), outbox(), partition :: non_neg_integer()) :: :ok

  @optional_callbacks notify: 3

  @doc false
  @spec for_repo!(module()) :: module()
  def for_repo!(repo) do
    if not (Code.ensure_loaded?(repo) and function_exported?(repo, :__adapter__, 0)) do
      raise ArgumentError, "expected :repo to be an Ecto.Repo module, got: #{inspect(repo)}"
    end

    case repo.__adapter__() do
      Ecto.Adapters.Postgres ->
        Outboxer.Storage.Postgres

      Ecto.Adapters.SQLite3 ->
        Outboxer.Storage.SQLite

      other ->
        raise ArgumentError,
              "Outboxer has no storage for #{inspect(other)} (repo #{inspect(repo)}). " <>
                "Supported adapters are Ecto.Adapters.Postgres and Ecto.Adapters.SQLite3; " <>
                "pass :storage to use your own Outboxer.Storage implementation."
    end
  end
end
