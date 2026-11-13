# Kafka relay

`Outboxer.Relay.Kafka` produces to Kafka with [brod](https://hex.pm/packages/brod). It is compiled
only when `:brod` is among your dependencies.

```elixir
{:brod, "~> 4.0"}
```

brod's dependencies include a NIF that needs `cmake` at build time.

## Setup

Use a brod client you already start:

```elixir
orders: [relay: {Outboxer.Relay.Kafka, client: :my_brod_client, topic: "orders"}]
```

or let Outboxer start and supervise one, by giving it endpoints:

```elixir
orders: [
  relay:
    {Outboxer.Relay.Kafka,
     client: :outboxer_orders,
     endpoints: [{"kafka-1", 9092}, {"kafka-2", 9092}],
     client_config: [ssl: true, sasl: {:plain, "user", "secret"}],
     producer_config: [required_acks: -1, compression: :snappy],
     topic: "orders"}
]
```

The client is a child of that outbox's supervisor and is started before its workers. A topic's
producer is started the first time the topic is used, with `:producer_config`.

Route by message with a function or MFA:

```elixir
topic: fn envelope -> "events." <> envelope.type end
topic: {MyApp.Routing, :topic_for, []}
```

A group must always resolve to the same topic, or its order is only kept per topic.

## Mapping

| Kafka record | From |
| --- | --- |
| key | the group key, or empty |
| value | the payload, untouched |
| headers | `outboxer-id`, `outboxer-type`, then your headers |
| partition | hash of the group key; random without one |

## Good to know

- **Order.** Same group key, same Kafka partition, sent with one synchronous request per
  topic-partition and acknowledged before the rows are removed. Kafka then keeps that order for
  consumers.
- **Keep `required_acks: -1`** (the default here). With `1` or `0` a broker failover can lose
  messages Outboxer already deleted.
- **Duplicates.** A batch can be produced twice after a failure between "acked" and "deleted".
  Deduplicate on the `outboxer-id` header if you need to.
- **Partitioning is done by Outboxer**, with `:erlang.phash2/2`, not by brod and not with Kafka's
  default murmur2. Other producers writing the same keys to the same topic will not agree on the
  partition. Pass a `:partitioner` function if they must.
- **Adding partitions to a topic moves keys**, as with any hash partitioner. Drain the outbox first
  if order across that change matters.
- **Outbox partitions and Kafka partitions are unrelated.** The first is how many workers deliver
  in parallel, the second is the topic's layout.
- **Message size.** A batch that exceeds the broker's limits fails as a whole and is retried as a
  whole. Lower `batch_size` if your payloads are large.

## Not a fit when

- You need Kafka transactions or exactly-once semantics across topics.
- You produce at a volume where an extra row per message strains the database: it, not Kafka,
  becomes the limit. Consider CDC.
