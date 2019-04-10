defmodule Mix.Tasks.PublishMessages do
  use Mix.Task

  @shortdoc "Publish 100.000 messages to RabbitMQ"
  def run(_args) do
    {:ok, conn} = AMQP.Connection.open
    {:ok, chan} = AMQP.Channel.open(conn)
    AMQP.Queue.declare chan, "broadway"
    AMQP.Exchange.declare chan, "default"
    AMQP.Queue.bind chan, "broadway", "default"

    publish_messages(chan, 100_000)
  end

  defp publish_messages(chan, count) do
    (1..count)
    |> Enum.each(fn i -> AMQP.Basic.publish chan, "default", "", generate_payload(i) end)
  end

  defp generate_payload(count) do
    Jason.encode!(%{uuid: Ecto.UUID.generate(), payload: %{number: count}})
  end
end
