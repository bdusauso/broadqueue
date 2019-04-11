# Broadqueue

Experiment with Broadway/RabbitMQ

## Introduction

I often have the use-case of fetching messages from RabbitMQ, processing them and storing them in the database, *while keeping their order at the same time*. 
That implies only one consumer - thus no concurrency - and very limited throughput. 
I've tried many solution but the best fit for this specific use-case was to use _batch processing_ and _batch insert_ into the database.

I've never found a good solution by myself. That is until [GenStage](https://github.com/elixir-lang/gen_stage) came out. And then [Broadway](https://github.com/plataformatec/broadway)

## Usage

* Clone this repository;
* Launch `docker-compose -d up`;
* Setup the database: `mix ecto.setup`;
* Publish some messages: `mix publish_messages --count 100000`
* Consume the messages: `mix run --no-halt`

### Batch size

By default the batch size is 100 but you can modify it by setting the `BATCH_SIZE` environment variable: `BATCH_SIZE=250 mix run --no-halt` 