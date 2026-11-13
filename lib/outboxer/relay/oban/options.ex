defmodule Outboxer.Relay.Oban.Options do
  @moduledoc false

  @schema [
    worker: [
      type: {:or, [:atom, {:fun, 1}]},
      required: true,
      doc:
        "The `Oban.Worker` module that handles the message, or a function taking the envelope and returning one."
    ],
    oban: [
      type: :atom,
      default: Oban,
      doc: "Name of the Oban instance to insert into."
    ],
    payload: [
      type: {:in, [:json, :raw, :base64]},
      default: :json,
      doc: """
      How the payload is placed in the job args under `"payload"`. `:json` decodes it (use with the \
      default message encoding), `:raw` passes the string through, `:base64` encodes binary payloads.
      """
    ],
    args: [
      type: {:or, [{:fun, 1}, nil]},
      default: nil,
      doc:
        "A function taking the envelope and returning the job args map. Replaces the default args entirely."
    ],
    job_opts: [
      type: {:or, [:keyword_list, {:fun, 1}]},
      default: [],
      doc:
        "Options for `c:Oban.Worker.new/2` (`:queue`, `:priority`, `:max_attempts`, ...), or a function taking the envelope and returning them."
    ]
  ]

  def schema, do: @schema
end
