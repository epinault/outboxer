defmodule Outboxer.Envelope do
  @moduledoc """
  A stored outbox message, as handed to a relay.

  The envelope is what `c:Outboxer.Relay.deliver/2` receives. `payload` is the
  binary produced by `c:Outboxer.Message.encode/1`; Outboxer never looks inside it.

  `attempts` is the number of failed delivery attempts so far and `last_error`
  the reason for the most recent one, so both are `0` and `nil` on a first try.
  """

  @enforce_keys [:id, :outbox, :type, :payload]
  defstruct [
    :id,
    :outbox,
    :type,
    :group_key,
    :payload,
    :inserted_at,
    :last_error,
    partition: 0,
    headers: %{},
    attempts: 0
  ]

  @type t :: %__MODULE__{
          id: pos_integer(),
          outbox: atom(),
          type: String.t(),
          group_key: String.t() | nil,
          payload: binary(),
          headers: %{optional(String.t()) => String.t()},
          partition: non_neg_integer(),
          attempts: non_neg_integer(),
          inserted_at: DateTime.t() | nil,
          last_error: String.t() | nil
        }

  @doc """
  A stable identifier for this message, suitable as an idempotency or
  deduplication key on the consuming side.

  Delivery is at-least-once, so a consumer can see the same message twice. The
  id is the same on every attempt.

      iex> Outboxer.Envelope.dedup_id(%Outboxer.Envelope{id: 42, outbox: :orders, type: "t", payload: ""})
      "orders-42"
  """
  @spec dedup_id(t()) :: String.t()
  def dedup_id(%__MODULE__{outbox: outbox, id: id}), do: "#{outbox}-#{id}"
end
