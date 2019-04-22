defmodule Mix.Tasks.PublishMessages do
  use Mix.Task

  @chunk_size 10_000

  defmodule Configuration do
    alias __MODULE__

    defstruct count: 0,
              fault_ratio: 0,
              duplicate_ratio: 0

    def new(count, fault_ratio, duplicate_ratio) do
      config = %Configuration{
        count: count,
        fault_ratio: fault_ratio,
        duplicate_ratio: duplicate_ratio
      }  

      if valid?(config), do: config, else: raise OptionParser.ParseError
    end

    def valid?(%Configuration{} = config) do
      config.count > 0 && 
      config.fault_ratio >= 0 && 
      config.fault_ratio <= 100 &&
      config.duplicate_ratio >= 0 &&
      config.duplicate_ratio <= 100 &&
      config.fault_ratio + config.duplicate_ratio <= 100
    end
  end

  alias Configuration

  @shortdoc "Publish messages to RabbitMQ"
  def run(args) do
    case parse_cli_args(args) do
      {:ok, config} ->
        {:ok, conn} = AMQP.Connection.open
        {:ok, chan} = AMQP.Channel.open(conn)
        AMQP.Queue.declare(chan, "broadway")
        AMQP.Exchange.declare(chan, "default")
        AMQP.Queue.bind(chan, "broadway", "default")
        AMQP.Confirm.select(chan)
        publish_messages(chan, config)

      :error ->
        Mix.Shell.IO.error("""
          Usage: mix publish_messages --count <messages_count> --fault-ratio <percentage> --duplicate-ratio <percentage>
        """)
    end
  end

  defp publish_messages(chan, config) do
    config
    |> generate_payloads()
    |> Stream.chunk_every(@chunk_size)
    |> Enum.each(&(publish_chunk(&1, chan, config)))
  end

  defp publish_chunk(chunk, chan, config) do
    Enum.each(chunk, &(AMQP.Basic.publish chan, "default", "", &1))
    AMQP.Confirm.wait_for_confirms(chan)
  end

  defp generate_payloads(config) do
    duplicates = List.duplicate(generate_payload(config), (div(config.count, 100)) * config.duplicate_ratio) 
    faults = List.duplicate("faulty_string", (div(config.count, 100)) * config.fault_ratio)
    valids = for _ <- 1..(config.count - length(duplicates) - length(faults)), do: generate_payload(config)

    payloads = 
      (duplicates ++ faults ++ valids)
      |> Enum.shuffle()
      |> Stream.with_index()
      |> Stream.map(fn 
        {"faulty_string", _} -> "faulty_string"
        {map, i} -> map |> Map.put_new(:payload, %{number: i}) |> Jason.encode!
      end)
      |> Enum.to_list()
  end

  defp generate_payload(config) do
    %{uuid: Ecto.UUID.generate()}
  end

  defp parse_cli_args(cli_args) do
    try do
      switches = [
        count: :integer, 
        fault_ratio: :integer, 
        duplicate_ratio: :integer
      ]
      {parsed_args, _}
        = OptionParser.parse!(cli_args, switches: switches)

      config = Configuration.new(
        Keyword.get(parsed_args, :count, 10),
        Keyword.get(parsed_args, :fault_ratio, 0),
        Keyword.get(parsed_args, :duplicate_ratio, 0)  
      )


      {:ok, config}
    rescue
      _ ->
        :error
    end
  end
end
