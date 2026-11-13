defmodule Outboxer.Storage.Record do
  @moduledoc false
  # Ecto schema of an outbox row. The table name is configurable, so queries
  # always use `{table, Record}` as their source.

  use Ecto.Schema

  alias Outboxer.Envelope

  @type t :: %__MODULE__{}

  schema "outboxer_messages" do
    field(:outbox, :string)
    field(:partition, :integer, default: 0)
    field(:group_key, :string)
    field(:type, :string)
    field(:payload, :binary)
    field(:headers, :string)
    field(:status, :string, default: "pending")
    field(:attempts, :integer, default: 0)
    field(:last_error, :string)
    field(:scheduled_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
    field(:delivered_at, :utc_datetime_usec)
  end

  @spec to_envelope(t() | map(), atom()) :: Envelope.t()
  def to_envelope(record, outbox) do
    %Envelope{
      id: record.id,
      outbox: outbox,
      partition: record.partition,
      group_key: record.group_key,
      type: record.type,
      payload: record.payload,
      headers: decode_headers(record.headers),
      attempts: record.attempts,
      last_error: record.last_error,
      inserted_at: record.inserted_at
    }
  end

  @spec encode_headers(map()) :: String.t() | nil
  def encode_headers(headers) when map_size(headers) == 0, do: nil
  def encode_headers(headers), do: JSON.encode!(headers)

  defp decode_headers(nil), do: %{}
  defp decode_headers(json), do: JSON.decode!(json)
end
