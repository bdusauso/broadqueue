defmodule Broadqueue.Event do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "events" do
    field :payload, :map
  end
end
