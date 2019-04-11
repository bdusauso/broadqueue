defmodule Mix.Tasks.PublishMessages do
  use Mix.Task

  @shortdoc "Publish 100.000 messages to RabbitMQ"
  def run(args) do
    case messages_count(args) do
      {:ok, count} ->
        {:ok, conn} = AMQP.Connection.open
        {:ok, chan} = AMQP.Channel.open(conn)
        AMQP.Queue.declare(chan, "broadway")
        AMQP.Exchange.declare(chan, "default")
        AMQP.Queue.bind(chan, "broadway", "default")
        AMQP.Confirm.select(chan)
        publish_messages(chan, count)
        AMQP.Confirm.wait_for_confirms(chan)

      :error ->
        Mix.Shell.IO.error("Usage: mix publish_messages --count <messages_count>")
    end
  end

  defp publish_messages(chan, count) do
    (1..count)
    |> Enum.each(fn i -> AMQP.Basic.publish chan, "default", "", generate_payload(i) end)
  end

  defp generate_payload(count) do
    Jason.encode!(%{uuid: Ecto.UUID.generate(), payload: %{number: count}})
  end

  defp messages_count(cli_args) do
    case OptionParser.parse!(cli_args, strict: [count: :integer]) do
      {[count: count], _} ->
        {:ok, count}

      _ ->
        :error
    end
  end
end
