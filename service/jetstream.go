package main

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"os"
	"time"
)

func CreateStream() (*nats.Conn, jetstream.ConsumeContext) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("nats connect err:", err.Error())
		os.Exit(1)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("jetstream.new err:", err.Error())
		nc.Close()
		os.Exit(1)
	}

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"ORDERS.*"},
	})
	if err != nil {
		slog.Error("create stream err: ", err.Error())
		nc.Close()
		os.Exit(1)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "CONS",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		slog.Error("create consumer err: %v", err.Error())
		nc.Close()
		os.Exit(1)
	}

	msgs, err := consumer.Consume(Jsonread)
	if err != nil {
		slog.Error("couldn't consume messages: ", err.Error())
		nc.Close()
		os.Exit(1)
	}
	return nc, msgs
}
