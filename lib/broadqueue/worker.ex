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
      ],
      batchers: [
        storage: [stages: 1, batch_size: batch_size],
        dead_letter: []
      ]
    )
  end

  @impl true
  def handle_message(_, message, _) do
    try do
      message
      |> Message.update_data(&decode_data/1)
      |> Message.put_batcher(:storage)
    rescue
      _ -> Message.put_batcher(message, :dead_letter)
    end
  end

  @impl true
  def handle_batch(:storage, messages, _batch_info, _context) do
    events =
      messages
      |> Stream.with_index()
      |> Flow.from_enumerable(max_demand: 1)
      |> Flow.map(&process_batch_message/1)
      |> Enum.sort_by(fn {_, index} -> index end)
      |> Enum.map(fn {message, _} -> message.data end)

    Repo.insert_all(Event, events)

    messages
  end

  def handle_batch(:dead_letter, messages, _, _) do
    Logger.error("Discarding #{length(messages)} messages")
    messages
  end

  defp decode_data(data) do
    data
    |> Jason.decode!
    |> atomize_keys
  end

  defp atomize_keys(map = %{}) do
    map
    |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
    |> Enum.into(%{})
  end

  defp process_batch_message({_message, _} = indexed_message) do
    indexed_message
  end
end
