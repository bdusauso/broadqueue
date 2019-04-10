defmodule Broadqueue.Producer do
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
    |> Message.put_batcher(:storage)
  end

  @impl true
  def handle_batch(:storage, messages, _batch_info, _context) do
    events =
      messages
      |> Stream.map(&(&1.data))
      |> Stream.map(&Jason.decode!/1)
      |> Stream.map(&atomize_keys/1)
      |> Enum.to_list

    Repo.insert_all(Event, events)

    messages
  end

  defp atomize_keys(map = %{}) do
    map
    |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
    |> Enum.into(%{})
  end
end
