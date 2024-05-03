package main

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"os"
	"strconv"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		fmt.Printf(" nats connect err: %v", err.Error())
	}

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Printf("jetstream.new err: %v", err.Error())
	}

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"ORDERS.*"},
	})
	if err != nil {
		fmt.Printf("create stream err: %v", err.Error())
	}

	var filename string
	var file []byte
	for i := 0; i < 6; i++ {
		filename = "model" + strconv.Itoa(i) + ".json"
		file, _ = os.ReadFile(filename)
		js.Publish(ctx, "ORDERS.new", file)
	}
}
