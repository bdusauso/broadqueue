defmodule Broadqueue.Producer do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: {BroadwayRabbitMQ.Producer,
            queue: "broadway",
          },
          stages: 1
        ]
      ],
      processors: [
        default: [
          stages: 1
        ]
      ],
      batchers: [
        storage: [stages: 1, batch_size: 10]
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
    IO.inspect(messages, label: "Received batch")
    messages
  end
end
