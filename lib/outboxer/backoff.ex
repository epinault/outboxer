defmodule Outboxer.Backoff do
  @moduledoc false

  @doc """
  Milliseconds to wait before attempt `attempt + 1`, given that `attempt`
  attempts have failed so far.

  With a keyword spec the delay doubles from `:min` up to `:max`, with up to
  10% jitter so that retries from several groups do not line up.
  """
  @spec delay((pos_integer() -> non_neg_integer()) | keyword(), pos_integer()) ::
          non_neg_integer()
  def delay(fun, attempt) when is_function(fun, 1), do: max(fun.(attempt), 0)

  def delay(spec, attempt) when is_list(spec) do
    min = Keyword.fetch!(spec, :min)
    max = Keyword.fetch!(spec, :max)
    # Cap the exponent so the shift cannot produce a huge integer.
    base = min(max, min * Bitwise.bsl(1, min(attempt - 1, 30)))
    jitter = if base > 0, do: :rand.uniform(div(base, 10) + 1) - 1, else: 0
    min(max, base + jitter)
  end
end
