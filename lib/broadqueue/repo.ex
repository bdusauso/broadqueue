defmodule Broadqueue.Repo do
  use Ecto.Repo,
    otp_app: :broadqueue,
    adapter: Ecto.Adapters.Postgres
end
