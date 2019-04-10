defmodule Broadqueue.Event do
  use Ecto.Schema

  schema "events" do
    field :uuid, Ecto.UUID
    field :payload, :map
  end
end
