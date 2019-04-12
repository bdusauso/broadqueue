defmodule Mix.Tasks.PublishMessages do
  use Mix.Task

  @shortdoc "Publish messages to RabbitMQ"
  def run(args) do
    case parse_cli_args(args) do
      {:ok, {count, fault_ratio}} ->
        {:ok, conn} = AMQP.Connection.open
        {:ok, chan} = AMQP.Channel.open(conn)
        AMQP.Queue.declare(chan, "broadway")
        AMQP.Exchange.declare(chan, "default")
        AMQP.Queue.bind(chan, "broadway", "default")
        AMQP.Confirm.select(chan)
        publish_messages(chan, count, fault_ratio)
        AMQP.Confirm.wait_for_confirms(chan)

      :error ->
        Mix.Shell.IO.error("Usage: mix publish_messages --count <messages_count> --fault-ratio <percentage>")
    end
  end

  defp publish_messages(chan, count, fault_ratio) do
    (1..count)
    |> Enum.each(fn i -> AMQP.Basic.publish chan, "default", "", generate_payload(i, fault_ratio) end)
  end

  defp generate_payload(count, fault_ratio) do
    if :rand.uniform(100) >= fault_ratio do
      Jason.encode!(%{uuid: Ecto.UUID.generate(), payload: %{number: count}})
    else
      "faulty_string"
    end
  end

  defp parse_cli_args(cli_args) do
    try do
      {parsed_args, _}
        = OptionParser.parse!(cli_args, switches: [count: :integer, fault_ratio: :integer]) |> IO.inspect

      count = Keyword.get(parsed_args, :count, 10)
      fault_ratio = Keyword.get(parsed_args, :fault_ratio, 0)
      if count <= 0 || fault_ratio < 0 || fault_ratio > 100, do: raise OptionParser.ParseError

      {:ok, {count, fault_ratio}}
    rescue
      _ ->
        :error
    end
  end
end
