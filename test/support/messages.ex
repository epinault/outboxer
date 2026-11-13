defmodule Outboxer.Test.Event do
  @moduledoc false
  use Outboxer.Message

  defstruct [:n, :group]

  @impl Outboxer.Message
  def group_key(%__MODULE__{group: group}), do: group
end

defmodule Outboxer.Test.Custom do
  @moduledoc false
  use Outboxer.Message, outbox: :custom, type: "custom.v1"

  defstruct [:body, :trace]

  @impl Outboxer.Message
  def encode(%__MODULE__{body: body}), do: "raw:" <> body

  @impl Outboxer.Message
  def headers(%__MODULE__{trace: trace}), do: %{"trace" => trace}
end

defmodule Outboxer.Test.Order do
  @moduledoc false
  use Outboxer.Message, outbox: :orders

  defstruct [:n]
end
