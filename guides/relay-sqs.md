# SQS relay

`Outboxer.Relay.SQS` sends to Amazon SQS with [ex_aws_sqs](https://hex.pm/packages/ex_aws_sqs). It
is compiled only when `:ex_aws_sqs` is among your dependencies.

```elixir
{:ex_aws, "~> 2.5"},
{:ex_aws_sqs, "~> 3.4"},
{:sweet_xml, "~> 0.7"},    # or :saxy; needed to read which batch entries SQS accepted
{:hackney, "~> 1.20"}      # or configure another ex_aws HTTP client
```

## Setup

```elixir
orders: [
  relay: {Outboxer.Relay.SQS, queue_url: "https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo"}
]
```

Credentials and region come from your ex_aws configuration; override per relay with
`ex_aws_config: [region: "eu-west-1"]`. For LocalStack or MiniStack:

```elixir
ex_aws_config: [scheme: "http://", host: "localhost", port: 4566, region: "us-east-1"]
```

## Standard or FIFO

| | Standard queue | FIFO queue (`.fifo`) |
| --- | --- | --- |
| Order | **not kept by SQS** | kept per `MessageGroupId` |
| Group key | ignored | sent as `MessageGroupId` |
| Duplicates | consumer must handle | SQS drops repeats for 5 minutes (deduplication id) |
| Throughput | effectively unlimited | 300 requests/s per queue (3,000 messages with batching), more with high-throughput mode |

Use a standard queue with `ordering: :none` when you only want the transactional guarantee. Use a
FIFO queue when order matters; an ordered outbox on a standard queue buys you nothing.

Messages without a group key on a FIFO queue get their own unique group id, so they are not
serialised behind each other.

## Batching on FIFO queues

SQS can accept some entries of a `SendMessageBatch` and reject others. If two messages of one group
were in the same request, the second could be accepted after the first was rejected.

- `batching: :group_safe` (default) never puts two messages of the same group in one request.
  Different groups still share requests. A single very active group is sent one message per
  request.
- `batching: :fast` fills every request with up to 10 messages. Faster for hot groups, with that
  small reordering window on partial failures.

## Good to know

- **Payload.** SQS bodies are text. The default JSON encoding is fine; for binary payloads set
  `encoding: :base64`. A non-UTF-8 payload with `encoding: :raw` fails on every attempt.
- **Headers** become string message attributes, plus `outboxer-id` and `outboxer-type`. SQS allows
  10 attributes per message.
- **Size.** Oversized messages are rejected by SQS on every attempt. With the default
  `max_attempts: :infinity` that blocks the group, so set `max_attempts` if you cannot bound your
  payload size.
- **Without an XML parser** ex_aws returns the raw response, Outboxer cannot tell what was accepted,
  and every batch is treated as failed. The error message says so.

## Not a fit when

- You need ordering on a standard queue, or more than FIFO throughput for one group.
- You need SNS fan-out: publish to a queue subscribed to the topic, or write a small relay with
  `ExAws.SNS` (see [Custom relays](custom-relays.md)).
