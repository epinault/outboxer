defmodule Outboxer.Relay.Function do
  @moduledoc """
  A relay that calls a function for every message.

  Useful for custom sinks, for wiring Outboxer to something that has no
  dedicated relay, and for trying the library out.

      {Outboxer,
       repo: MyApp.Repo,
       outboxes: [
         audit: [relay: {Outboxer.Relay.Function, fun: &MyApp.Audit.forward/1}]
       ]}

  ## Options

    * `:fun` (required) - a 1-arity function, or an `{module, function, extra_args}`
      tuple called as `apply(module, function, [envelope | extra_args])`. It
      receives an `Outboxer.Envelope` and returns `:ok`, `{:ok, term}` or
      `{:error, reason}`. Raising counts as an error.

  Messages are delivered one at a time, in order, and a failure blocks the
  rest of that message's group (see `Outboxer.Relay.sequential/2`).

  Prefer an MFA or a named capture (`&Mod.fun/1`) over an anonymous function
  when the supervisor options come from config files.
  """

  @behaviour Outboxer.Relay

  @impl Outboxer.Relay
  def init(opts) do
    case Keyword.fetch(opts, :fun) do
      {:ok, fun} when is_function(fun, 1) ->
        {:ok, fun}

      {:ok, {module, function, args}}
      when is_atom(module) and is_atom(function) and is_list(args) ->
        {:ok, fn envelope -> apply(module, function, [envelope | args]) end}

      _other ->
        {:error, "Outboxer.Relay.Function requires :fun to be a 1-arity function or an MFA tuple"}
    end
  end

  @impl Outboxer.Relay
  def deliver(envelopes, fun), do: Outboxer.Relay.sequential(envelopes, fun)
end
