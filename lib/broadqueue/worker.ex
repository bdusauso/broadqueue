defmodule Broadqueue.Worker do
  use Broadway
  require Logger

  alias Broadway.Message
  alias Broadqueue.{Event, Repo}

  def start_link(_opts) do
    batch_size =
      case System.get_env("BATCH_SIZE") do
        nil ->
          100

        string_size ->
          String.to_integer(string_size)
      end

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: {
            BroadwayRabbitMQ.Producer,
            queue: "broadway",
            qos: [prefetch_count: batch_size]
          },
          stages: 1,
        ]
      ],
      processors: [
        default: [
          stages: 1
        ]
      ]
    )
  end

  @impl true
  def handle_message(_, message, _) do
    Event
    |> struct!(message |> decode_data())
    |> Repo.insert!()

    message
  end

  defp decode_data(%Message{data: data}) do
    data
    |> Jason.decode!
    |> atomize_keys
  end

  defp atomize_keys(map = %{}) do
    map
    |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
    |> Enum.into(%{})
  end
end
