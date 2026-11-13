defmodule Outboxer.Config do
  @moduledoc false
  # Validated, immutable configuration of one Outboxer instance.
  #
  # Built once from the options given to `Outboxer.start_link/1` and stored in
  # `:persistent_term` under the instance name, so that `Outboxer.publish/2`
  # can find it from any process without a GenServer call.

  alias Outboxer.Config.Outbox

  @enforce_keys [:name, :repo, :storage]
  defstruct [
    :name,
    :repo,
    :storage,
    :prefix,
    table: "outboxer_messages",
    notify: false,
    testing: :disabled,
    require_transaction: false,
    prune_interval: 60_000,
    outboxes: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          repo: module(),
          storage: module(),
          prefix: String.t() | nil,
          table: String.t(),
          notify: boolean(),
          testing: :disabled | :manual,
          require_transaction: boolean(),
          prune_interval: pos_integer(),
          outboxes: %{optional(atom()) => Outbox.t()}
        }

  @outbox_schema [
    relay: [
      type: {:or, [:atom, {:tuple, [:atom, :keyword_list]}]},
      required: true,
      doc:
        "The relay that delivers this outbox's messages: a module implementing `Outboxer.Relay`, or `{module, opts}`."
    ],
    ordering: [
      type: {:in, [:group, :total, :none]},
      default: :group,
      doc: """
      `:group` keeps order between messages that share a group key (messages without one are unordered). \
      `:total` keeps one global order for the whole outbox (forces a single partition; one failing message \
      blocks everything behind it). `:none` ignores group keys entirely.
      """
    ],
    partitions: [
      type: :pos_integer,
      default: 1,
      doc: """
      How many partitions, and so how many concurrent delivery workers, the outbox has. A group always \
      lands in the same partition. Do not change this while the outbox holds undelivered messages.
      """
    ],
    batch_size: [
      type: :pos_integer,
      default: 100,
      doc: "Maximum number of messages fetched and handed to the relay at once."
    ],
    poll_interval: [
      type: :pos_integer,
      default: 1_000,
      doc: "Milliseconds an idle worker waits before looking for new messages."
    ],
    max_attempts: [
      type: {:or, [{:in, [:infinity]}, :pos_integer]},
      default: :infinity,
      doc: """
      `:infinity` retries a failing message forever, which blocks its group and never breaks order. \
      A number moves the message to the dead state after that many failed attempts and lets the group continue.
      """
    ],
    backoff: [
      type: {:or, [{:fun, 1}, :keyword_list]},
      default: [min: 1_000, max: 60_000],
      doc: """
      Delay before a failed message is retried. Either `[min: ms, max: ms]` for exponential backoff with \
      jitter, or a function taking the number of failed attempts and returning milliseconds.
      """
    ],
    retention: [
      type: {:or, [{:in, [:delete]}, {:tuple, [{:in, [:keep]}, :pos_integer]}]},
      default: :delete,
      doc: """
      `:delete` removes a message as soon as it is delivered (smallest table, recommended). \
      `{:keep, seconds}` marks it delivered and prunes it after that many seconds.
      """
    ],
    serialize_writes: [
      type: :boolean,
      default: false,
      doc: """
      Postgres only. Take a transaction-scoped advisory lock on the group key when publishing, so that \
      concurrent transactions writing to the same group commit in id order. Only needed when nothing else \
      (such as a row lock on the aggregate) already serialises those writers.
      """
    ]
  ]

  @schema [
    name: [
      type: :atom,
      default: Outboxer,
      doc: "Name of this instance. Only needed when you run more than one."
    ],
    repo: [
      type: :atom,
      required: true,
      doc:
        "The `Ecto.Repo` that holds the outbox table. It must be the repo your business writes use."
    ],
    outboxes: [
      type: :keyword_list,
      default: [],
      keys: [*: [type: :keyword_list, keys: @outbox_schema]],
      doc: "The outboxes, as `name: options`. See \"Outbox options\" below."
    ],
    table: [
      type: :string,
      default: "outboxer_messages",
      doc: "Name of the outbox table. Must match what was given to `Outboxer.Migration`."
    ],
    prefix: [
      type: {:or, [:string, nil]},
      default: nil,
      doc: "Postgres schema of the outbox table."
    ],
    storage: [
      type: :atom,
      doc: "An `Outboxer.Storage` module. Detected from the repo's adapter when omitted."
    ],
    notify: [
      type: :boolean,
      default: false,
      doc: """
      Postgres only. Wake workers through `LISTEN/NOTIFY` when a publishing transaction commits, instead \
      of waiting for the next poll. Uses one extra database connection.
      """
    ],
    testing: [
      type: {:in, [:disabled, :manual]},
      default: :disabled,
      doc: """
      `:manual` starts no workers: messages are stored but only delivered when you call \
      `Outboxer.Testing.drain/2`. Also the right setting for nodes that publish but must not relay.
      """
    ],
    require_transaction: [
      type: :boolean,
      default: false,
      doc: "Raise when `Outboxer.publish/2` is called outside a repo transaction."
    ],
    prune_interval: [
      type: :pos_integer,
      default: 60_000,
      doc:
        "Milliseconds between pruning passes, for outboxes using `retention: {:keep, seconds}`."
    ]
  ]

  @doc false
  def schema, do: @schema

  @doc false
  def outbox_schema, do: @outbox_schema

  @doc false
  def docs,
    do:
      NimbleOptions.docs(
        Keyword.put(@schema, :outboxes, Keyword.delete(@schema[:outboxes], :keys))
      )

  @doc false
  def outbox_docs, do: NimbleOptions.docs(@outbox_schema)

  @spec new!(keyword()) :: t()
  def new!(opts) do
    opts = NimbleOptions.validate!(opts, @schema)
    repo = Keyword.fetch!(opts, :repo)
    storage = Keyword.get_lazy(opts, :storage, fn -> Outboxer.Storage.for_repo!(repo) end)

    outboxes =
      opts
      |> Keyword.fetch!(:outboxes)
      |> Map.new(fn {name, outbox_opts} -> {name, Outbox.new!(name, outbox_opts)} end)

    fields =
      opts
      |> Keyword.take([
        :name,
        :repo,
        :table,
        :prefix,
        :notify,
        :testing,
        :require_transaction,
        :prune_interval
      ])
      |> Keyword.merge(storage: storage, outboxes: outboxes)

    struct!(__MODULE__, fields)
  end

  @spec put(t()) :: :ok
  def put(%__MODULE__{name: name} = conf), do: :persistent_term.put({Outboxer, name}, conf)

  @spec fetch!(atom()) :: t()
  def fetch!(name) do
    case :persistent_term.get({Outboxer, name}, nil) do
      %__MODULE__{} = conf ->
        conf

      nil ->
        raise ArgumentError,
              "no Outboxer instance named #{inspect(name)} is running. " <>
                "Add {Outboxer, repo: MyApp.Repo, outboxes: [...]} to your supervision tree."
    end
  end

  @spec outbox!(t(), atom()) :: Outbox.t()
  def outbox!(%__MODULE__{outboxes: outboxes, name: name}, outbox) do
    case outboxes do
      %{^outbox => config} ->
        config

      _other ->
        raise ArgumentError,
              "unknown outbox #{inspect(outbox)} for Outboxer instance #{inspect(name)}. " <>
                "Configured outboxes: #{inspect(Map.keys(outboxes))}"
    end
  end

  @spec registry(atom()) :: atom()
  # One atom per configured instance, created at boot: not an atom leak.
  # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
  def registry(name), do: Module.concat(name, Registry)
end
