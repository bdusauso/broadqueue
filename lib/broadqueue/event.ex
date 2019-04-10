defmodule Broadqueue.Event do
  use Ecto.Schema

  schema "events" do
    field :payload, :map
  end
end
