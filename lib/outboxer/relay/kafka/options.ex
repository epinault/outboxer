defmodule Outboxer.Relay.Kafka.Options do
  @moduledoc false
  # Kept outside the `if Code.ensure_loaded?(:brod)` block so the option docs
  # can be built without brod.

  @schema [
    client: [
      type: :atom,
      required: true,
      doc:
        "The brod client id. An existing client, or the name to register the one Outboxer starts."
    ],
    topic: [
      type: {:or, [:string, {:fun, 1}, :mfa]},
      required: true,
      doc: """
      The topic: a string, a function taking the envelope, or `{module, function, extra_args}` \
      called with the envelope first. A function must send a given group to a single topic.
      """
    ],
    endpoints: [
      type: {:or, [{:list, :any}, nil]},
      default: nil,
      doc:
        "Kafka bootstrap endpoints, such as `[{\"localhost\", 9092}]`. When set, Outboxer starts and supervises the brod client."
    ],
    client_config: [
      type: :keyword_list,
      default: [],
      doc:
        "brod client configuration (SSL, SASL, ...) for the client Outboxer starts. Ignored without `:endpoints`."
    ],
    producer_config: [
      type: :keyword_list,
      default: [required_acks: -1],
      doc: "brod producer configuration, used when a topic's producer is started."
    ],
    partitioner: [
      type: {:or, [{:in, [:hash]}, {:fun, 2}]},
      default: :hash,
      doc: """
      `:hash` hashes the group key over the topic's partitions. Or a function taking the envelope and \
      the partition count, returning a partition. It must be deterministic for a group.
      """
    ],
    brod: [
      type: :atom,
      default: :brod,
      doc: false
    ]
  ]

  def schema, do: @schema
end
