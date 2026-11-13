defmodule Outboxer.Message do
  @moduledoc """
  The behaviour every outbox message implements.

  A message is a plain struct. `use Outboxer.Message` gives it sensible
  defaults for every callback, so the smallest message is:

      defmodule MyApp.Events.OrderPlaced do
        use Outboxer.Message, outbox: :orders

        defstruct [:order_id, :total]
      end

  and a message that takes part in ordered delivery adds a group key:

      defmodule MyApp.Events.OrderPlaced do
        use Outboxer.Message, outbox: :orders, type: "order.placed"

        defstruct [:order_id, :total]

        @impl true
        def group_key(%__MODULE__{order_id: id}), do: "order:\#{id}"
      end

  ## Options for `use`

    * `:outbox` - the outbox this message is published to. Can be overridden
      per call with `Outboxer.publish(msg, outbox: :other)`.
    * `:type` - the type string stored with the message and passed to the
      relay. Defaults to the module name without the `Elixir.` prefix.

  ## Callbacks and their defaults

  | Callback | Default |
  | --- | --- |
  | `c:outbox/0` | the `:outbox` option, or `nil` (then you must pass `outbox:` to `publish`) |
  | `c:type/0` | the `:type` option, or the module name |
  | `c:encode/1` | `JSON.encode!/1` of the struct's fields |
  | `c:group_key/1` | `nil` (unordered) |
  | `c:headers/1` | `%{}` |

  ## Encoding

  The payload is stored as an opaque binary, so `c:encode/1` may return JSON,
  Protobuf, Avro or anything else. The default uses Elixir's built-in `JSON`
  module on `Map.from_struct/1`; if your struct holds values `JSON` cannot
  encode (other structs without a `JSON.Encoder` implementation, tuples, pids)
  implement `c:encode/1` yourself.
  """

  @doc "The outbox this message goes to by default."
  @callback outbox() :: atom() | nil

  @doc "A stable type name for this kind of message."
  @callback type() :: String.t()

  @doc "Serialises the message to the binary that will be stored and relayed."
  @callback encode(message :: struct()) :: binary()

  @doc """
  The ordering group of the message, or `nil` when it does not need ordering.

  Messages with the same group key in the same outbox are delivered in the
  order they were published. See the "Ordering and groups" guide.
  """
  @callback group_key(message :: struct()) :: String.t() | nil

  @doc "String headers stored with the message and passed to the relay."
  @callback headers(message :: struct()) :: %{optional(String.t()) => String.t()}

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Outboxer.Message

      @outboxer_outbox Keyword.get(opts, :outbox)
      @outboxer_type Keyword.get_lazy(opts, :type, fn -> inspect(__MODULE__) end)

      @impl Outboxer.Message
      def outbox, do: @outboxer_outbox

      @impl Outboxer.Message
      def type, do: @outboxer_type

      @impl Outboxer.Message
      def encode(message), do: message |> Map.from_struct() |> JSON.encode!()

      @impl Outboxer.Message
      def group_key(_message), do: nil

      @impl Outboxer.Message
      def headers(_message), do: %{}

      defoverridable outbox: 0, type: 0, encode: 1, group_key: 1, headers: 1
    end
  end

  @doc false
  @spec message?(term()) :: boolean()
  def message?(%module{}) do
    Code.ensure_loaded?(module) and function_exported?(module, :encode, 1) and
      function_exported?(module, :group_key, 1)
  end

  def message?(_other), do: false
end
