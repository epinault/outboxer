defmodule Outboxer.Relay.SQS.Options do
  @moduledoc false

  @schema [
    queue_url: [
      type: :string,
      required: true,
      doc: "The queue URL. A URL ending in `.fifo` is treated as a FIFO queue."
    ],
    fifo: [
      type: {:or, [:boolean, nil]},
      default: nil,
      doc: "Whether the queue is FIFO. Detected from the `.fifo` suffix of the URL when omitted."
    ],
    batching: [
      type: {:in, [:group_safe, :fast]},
      default: :group_safe,
      doc: """
      FIFO queues only. `:group_safe` never puts two messages of the same group in one \
      `SendMessageBatch` request, so a partial failure cannot reorder a group. `:fast` fills every \
      request with up to 10 messages regardless of group.
      """
    ],
    encoding: [
      type: {:in, [:raw, :base64]},
      default: :raw,
      doc:
        "`:raw` sends the payload as the message body and requires it to be valid UTF-8. `:base64` encodes it first."
    ],
    ex_aws_config: [
      type: :keyword_list,
      default: [],
      doc: "Overrides passed to `ExAws.request/2`, such as `[region: \"eu-west-1\"]`."
    ],
    client: [
      type: :atom,
      default: ExAws,
      doc: false
    ]
  ]

  def schema, do: @schema
end
