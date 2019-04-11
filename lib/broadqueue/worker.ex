defmodule Broadqueue.Worker do
  use Broadway

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
      ],
      batchers: [
        storage: [stages: 1, batch_size: batch_size]
      ]
    )
  end

  @impl true
  def handle_message(_, message, _) do
    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:storage)
  end

  @impl true
  def handle_batch(:storage, messages, _batch_info, _context) do
    Repo.insert_all(Event, Enum.map(messages, &(&1.data)))

    messages
  end

  defp process_data(data) do
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
