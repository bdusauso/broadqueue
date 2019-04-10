defmodule Broadqueue.Repo.Migrations.CreateEvents do
  use Ecto.Migration

  def change do
    create table("events") do
      add :payload, :map
    end
  end
end
