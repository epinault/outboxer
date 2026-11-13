defmodule Outboxer do
  @moduledoc """
  A transactional outbox for Ecto.

  You write a message to the outbox table **in the same database transaction**
  as your business data. Outboxer then relays it to Kafka, SQS, Oban or your
  own sink, in order, retrying until it gets through. The message exists if and
  only if the transaction committed, which is the guarantee that "write to the
  database, then publish to the broker" cannot give you.

  ## Getting started

  1. Create the table (see `Outboxer.Migration`):

         def up, do: Outboxer.Migration.up()
         def down, do: Outboxer.Migration.down()

  2. Define a message (see `Outboxer.Message`):

         defmodule MyApp.Events.OrderPlaced do
           use Outboxer.Message, outbox: :orders
           defstruct [:order_id, :total]

           @impl true
           def group_key(%__MODULE__{order_id: id}), do: "order:\#{id}"
         end

  3. Add Outboxer to your supervision tree, after the repo. Outboxer has no
     application of its own; this child is the whole runtime:

         children = [
           MyApp.Repo,
           {Outboxer,
            repo: MyApp.Repo,
            outboxes: [
              orders: [relay: {Outboxer.Relay.Kafka, client: :my_brod_client, topic: "orders"}]
            ]}
         ]

  4. Publish inside your transaction:

         MyApp.Repo.transaction(fn ->
           order = MyApp.Repo.insert!(changeset)
           {:ok, _envelope} = Outboxer.publish(%OrderPlaced{order_id: order.id, total: order.total})
           order
         end)

  ## Options

  #{Outboxer.Config.docs()}

  ## Outbox options

  Each entry under `:outboxes` accepts:

  #{Outboxer.Config.outbox_docs()}

  ## What gets started

  Only what the configuration needs:

      Outboxer (Supervisor)
      ├── Registry                       worker lookup
      ├── Outboxer.Notifier              only with notify: true
      ├── one supervisor per outbox
      │   ├── the relay's own children   e.g. a brod client, if the relay starts one
      │   └── one worker per partition
      └── Outboxer.Pruner                only if an outbox uses retention: {:keep, _}

  With `relay: false` or `testing: :manual` none of these are started and
  `publish/2` still works.

  ## Publishing and relaying on different nodes

  Start the same outboxes everywhere and switch delivery off where it is not
  wanted, for example on web pods:

      {Outboxer,
       repo: MyApp.Repo,
       relay: System.get_env("OUTBOXER_RELAY", "true") == "true",
       outboxes: [...]}

  A `relay: false` instance only publishes. It ignores the `:relay` of its
  outboxes, so it needs neither broker settings nor the broker's dependency.
  See the operations guide for what the two sides must agree on.

  ## One table per outbox

  Every outbox can live in its own table:

      outboxes: [
        orders: [table: "order_outbox", relay: ...],
        invoices: [table: "invoice_outbox", relay: ...]
      ]

  Create each one with `Outboxer.Migration.up(table: "order_outbox")`. Outboxes
  without `:table` share the instance's table. Message ids are unique within a
  table, not across tables.

  ## Several instances

  All functions take a `:name` option, defaulting to `Outboxer`. Run a second
  instance (for another repo, say) by giving it a name:

      {Outboxer, name: MyApp.BillingOutbox, repo: MyApp.BillingRepo, outboxes: [...]}

      Outboxer.publish(message, name: MyApp.BillingOutbox)

  ## Telemetry

  | Event | Measurements | Metadata |
  | --- | --- | --- |
  | `[:outboxer, :publish]` | `count` | `name`, `outboxes` |
  | `[:outboxer, :relay, :batch, :start]` | `system_time` | `name`, `outbox`, `partition`, `relay`, `count` |
  | `[:outboxer, :relay, :batch, :stop]` | `duration` | the above plus `fetched`, `delivered`, `failed`, `dead` |
  | `[:outboxer, :relay, :batch, :exception]` | `duration` | `name`, `outbox`, `partition`, `relay`, `kind`, `reason`, `stacktrace` |
  | `[:outboxer, :message, :retry]` | `attempt` | `name`, `outbox`, `id`, `type`, `group_key`, `error` |
  | `[:outboxer, :message, :dead]` | `attempt` | `name`, `outbox`, `id`, `type`, `group_key`, `error` |
  | `[:outboxer, :prune]` | `count` | `name`, `outbox` |
  """

  use Supervisor

  alias Outboxer.Config
  alias Outboxer.Config.Outbox
  alias Outboxer.Envelope
  alias Outboxer.Message
  alias Outboxer.Storage.Record
  alias Outboxer.Storage.SQL

  @type name :: atom()
  @type message :: struct()

  # -- supervision ------------------------------------------------------------

  @doc """
  Starts an Outboxer instance. See the module documentation for the options.

  Raises `ArgumentError` or `NimbleOptions.ValidationError` when the options
  are invalid, so that a misconfigured outbox fails at boot rather than at the
  first message.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    conf = Config.new!(opts)
    Supervisor.start_link(__MODULE__, conf, name: conf.name)
  end

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @impl Supervisor
  def init(%Config{} = conf) do
    Config.put(conf)
    Supervisor.init(children(conf), strategy: :one_for_one)
  end

  defp children(%Config{} = conf) do
    if Config.workers?(conf) do
      outboxes =
        for {_name, outbox} <- conf.outboxes, do: {Outboxer.OutboxSupervisor, {conf.name, outbox}}

      [{Registry, keys: :unique, name: Config.registry(conf.name)}] ++
        notifier(conf) ++ outboxes ++ pruner(conf)
    else
      []
    end
  end

  defp notifier(%Config{notify: true, storage: Outboxer.Storage.Postgres, name: name}) do
    [{Outboxer.Notifier, name}]
  end

  defp notifier(_conf), do: []

  defp pruner(%Config{} = conf) do
    if Enum.any?(conf.outboxes, fn {_name, outbox} -> outbox.retention != :delete end) do
      [{Outboxer.Pruner, conf.name}]
    else
      []
    end
  end

  # -- publishing -------------------------------------------------------------

  @doc """
  Writes one message, or a list of messages, to the outbox.

  Call it inside `Repo.transaction/2` together with your business writes. The
  insert uses the repo's current connection, so it commits or rolls back with
  everything else in that transaction.

  Returns `{:ok, envelope}` for a single message and `{:ok, envelopes}` for a
  list. Raises `ArgumentError` for something that is not a message or names an
  unknown outbox; database errors raise as they do for `Repo.insert_all/3`.

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
    * `:outbox` - overrides the message's `c:Outboxer.Message.outbox/0`.
    * `:group_key` - overrides the message's `c:Outboxer.Message.group_key/1`.
    * `:headers` - merged over the message's `c:Outboxer.Message.headers/1`.

  ## Outside a transaction

  Publishing without a surrounding transaction is allowed and stores the
  message atomically on its own. That is fine for a message that is not tied
  to another write. Set `require_transaction: true` on the instance if you
  would rather have that raise.
  """
  @spec publish(message() | [message()], keyword()) :: {:ok, Envelope.t() | [Envelope.t()]}
  def publish(message_or_messages, opts \\ [])

  def publish(messages, opts) when is_list(messages) do
    conf = Config.fetch!(Keyword.get(opts, :name, __MODULE__))
    in_transaction? = conf.repo.in_transaction?()

    if conf.require_transaction and not in_transaction? do
      raise ArgumentError,
            "Outboxer.publish/2 was called outside a transaction, and this instance has " <>
              "require_transaction: true. Wrap the call in #{inspect(conf.repo)}.transaction/2."
    end

    rows = Enum.map(messages, &to_row(conf, &1, opts))
    if in_transaction?, do: serialize_writes(conf, rows)
    envelopes = if rows == [], do: [], else: conf.storage.insert(conf, rows)

    :telemetry.execute(
      [:outboxer, :publish],
      %{count: length(envelopes)},
      %{name: conf.name, outboxes: rows |> Enum.map(& &1.outbox) |> Enum.uniq()}
    )

    announce(conf, rows, in_transaction?)
    {:ok, envelopes}
  end

  def publish(message, opts) do
    {:ok, [envelope]} = publish([message], opts)
    {:ok, envelope}
  end

  @doc """
  Adds a publish step to an `Ecto.Multi`.

  `message` is a message, a list of messages, or a function that receives the
  changes so far and returns either.

      Ecto.Multi.new()
      |> Ecto.Multi.insert(:order, changeset)
      |> Outboxer.publish_multi(:event, fn %{order: order} ->
        %OrderPlaced{order_id: order.id, total: order.total}
      end)
      |> MyApp.Repo.transaction()

  Takes the same options as `publish/2`.
  """
  @spec publish_multi(
          Ecto.Multi.t(),
          Ecto.Multi.name(),
          message() | [message()] | (map() -> term()),
          keyword()
        ) ::
          Ecto.Multi.t()
  def publish_multi(%Ecto.Multi{} = multi, step, message, opts \\ []) do
    Ecto.Multi.run(multi, step, fn _repo, changes ->
      resolved = if is_function(message, 1), do: message.(changes), else: message
      publish(resolved, opts)
    end)
  end

  defp to_row(%Config{} = conf, message, opts) do
    if not Message.message?(message) do
      raise ArgumentError,
            "expected a struct whose module uses Outboxer.Message, got: #{inspect(message)}"
    end

    module = message.__struct__
    outbox_name = Keyword.get(opts, :outbox) || module.outbox() || raise_no_outbox(module)
    outbox = Config.outbox!(conf, outbox_name)
    group_key = group_key(outbox, message, opts)

    %{
      outbox: outbox.name,
      partition: partition(outbox, group_key),
      group_key: group_key,
      type: module.type(),
      payload: encode!(module, message),
      headers: headers(module, message, opts)
    }
  end

  defp raise_no_outbox(module) do
    raise ArgumentError,
          "#{inspect(module)} does not name an outbox. Either `use Outboxer.Message, outbox: :name` " <>
            "or pass `outbox: :name` to Outboxer.publish/2."
  end

  defp group_key(%Outbox{ordering: :total}, _message, _opts), do: Outbox.total_group()
  defp group_key(%Outbox{ordering: :none}, _message, _opts), do: nil

  defp group_key(%Outbox{ordering: :group}, message, opts) do
    case Keyword.get_lazy(opts, :group_key, fn -> message.__struct__.group_key(message) end) do
      nil ->
        nil

      key when is_binary(key) ->
        key

      other ->
        raise ArgumentError,
              "group_key must be a string or nil, got: #{inspect(other)} for #{inspect(message.__struct__)}"
    end
  end

  # A group always maps to the same partition; ungrouped messages are spread.
  defp partition(%Outbox{partitions: 1}, _group_key), do: 0
  defp partition(%Outbox{partitions: partitions}, nil), do: :rand.uniform(partitions) - 1

  defp partition(%Outbox{partitions: partitions}, group_key),
    do: :erlang.phash2(group_key, partitions)

  defp encode!(module, message) do
    case module.encode(message) do
      payload when is_binary(payload) ->
        payload

      other ->
        raise ArgumentError,
              "#{inspect(module)}.encode/1 must return a binary, got: #{inspect(other)}"
    end
  end

  defp headers(module, message, opts) do
    message
    |> module.headers()
    |> Map.merge(Keyword.get(opts, :headers, %{}))
    |> Map.new(fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  # Sorted, so that two transactions locking the same groups cannot deadlock.
  defp serialize_writes(conf, rows) do
    rows
    |> Enum.filter(&(&1.group_key != nil and Config.outbox!(conf, &1.outbox).serialize_writes))
    |> Enum.map(&{&1.outbox, &1.group_key})
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.each(fn {outbox, group_key} -> conf.storage.lock_group(conf, outbox, group_key) end)
  end

  defp announce(conf, rows, in_transaction?) do
    partitions = rows |> Enum.map(&{&1.outbox, &1.partition}) |> Enum.uniq()

    if conf.notify and function_exported?(conf.storage, :notify, 3) do
      Enum.each(partitions, fn {outbox, partition} ->
        conf.storage.notify(conf, outbox, partition)
      end)
    end

    # Outside a transaction the rows are already committed, so the local
    # workers can be told right away. Inside one they are not visible yet.
    if not in_transaction? and Config.workers?(conf) do
      Enum.each(partitions, fn {outbox, partition} ->
        Outboxer.Worker.wake(conf.name, outbox, partition)
      end)
    end
  end

  # -- operations -------------------------------------------------------------

  @doc """
  Number of messages of an outbox by state: `%{pending: n, delivered: n, dead: n}`.

  `pending` is the backlog. `delivered` is only ever non-zero with
  `retention: {:keep, seconds}`.

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
  """
  @spec counts(atom(), keyword()) :: %{
          pending: non_neg_integer(),
          delivered: non_neg_integer(),
          dead: non_neg_integer()
        }
  def counts(outbox, opts \\ []) do
    conf = conf(opts)
    SQL.counts(conf, Config.outbox!(conf, outbox).name)
  end

  @doc """
  Lists dead messages of an outbox, oldest first, as envelopes.

  A message is dead when it failed `max_attempts` times. `envelope.last_error`
  holds the reason for the final failure.

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
    * `:limit` - maximum number of messages, default `100`.
  """
  @spec list_dead(atom(), keyword()) :: [Envelope.t()]
  def list_dead(outbox, opts \\ []), do: list(outbox, "dead", opts)

  @doc """
  Lists pending (not yet delivered) messages of an outbox, oldest first.

  Messages with `attempts > 0` have failed and are waiting for a retry; with
  ordering on, they are what is blocking their group.

  Takes the same options as `list_dead/2`.
  """
  @spec list_pending(atom(), keyword()) :: [Envelope.t()]
  def list_pending(outbox, opts \\ []), do: list(outbox, "pending", opts)

  defp list(outbox, status, opts) do
    conf = conf(opts)
    outbox = Config.outbox!(conf, outbox).name

    conf
    |> SQL.list(outbox, status, Keyword.get(opts, :limit, 100))
    |> Enum.map(&Record.to_envelope(&1, outbox))
  end

  @doc """
  Puts dead messages back in the outbox for delivery, with their attempt
  counter reset. Returns how many were revived.

  They keep their original id, so they rejoin their group in their original
  position. Messages of the same group published later may already have been
  delivered; that gap is what `max_attempts` trades for not blocking.

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
    * `:ids` - only these message ids. Default: every dead message of the outbox.
  """
  @spec retry_dead(atom(), keyword()) :: non_neg_integer()
  def retry_dead(outbox, opts \\ []) do
    conf = conf(opts)
    outbox = Config.outbox!(conf, outbox).name
    count = SQL.revive(conf, outbox, Keyword.get(opts, :ids, :all))
    if Config.workers?(conf), do: Outboxer.Worker.wake(conf.name, outbox)
    count
  end

  @doc """
  Permanently deletes messages of an outbox by id, whatever their state.
  Returns how many were deleted.

  This is how you skip a poison message that blocks a group under
  `max_attempts: :infinity`: find it with `list_pending/2` and discard it.

      Outboxer.discard(:orders, [4711])

  ## Options

    * `:name` - the Outboxer instance, default `Outboxer`.
  """
  @spec discard(atom(), [pos_integer()], keyword()) :: non_neg_integer()
  def discard(outbox, ids, opts \\ []) when is_atom(outbox) and is_list(ids) do
    conf = conf(opts)
    SQL.discard(conf, Config.outbox!(conf, outbox).name, ids)
  end

  defp conf(opts), do: Config.fetch!(Keyword.get(opts, :name, __MODULE__))
end
