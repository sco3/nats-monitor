package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Define command-line flags
	url := flag.String("url", "", "NATS server URL (long version)")
	u := flag.String("u", "nats://localhost:4222", "NATS server URL (short version)")
	streamName := flag.String("stream", "", "NATS stream name")
	flag.Parse()

	// Determine the NATS URL to use
	natsURL := *u
	if *url != "" {
		natsURL = *url
	}

	if *streamName == "" {
		log.Fatal("Stream name must be provided via the -stream flag.")
	}

	// Connect to NATS.
	log.Printf("Connecting to NATS server at %s...", natsURL)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("Successfully connected to NATS server.")

	// Create a JetStream context.
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Error creating JetStream context: %v", err)
	}

	// Get stream information to find the last sequence number.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.Stream(ctx, *streamName)
	if err != nil {
		log.Fatalf("Error getting stream info for '%s': %v", *streamName, err)
	}

	streamInfo, err := stream.Info(ctx)
	if err != nil {
		log.Fatalf("Error getting stream info for '%s': %v", *streamName, err)
	}
	streamLastSeq := streamInfo.State.LastSeq
	log.Printf("Checking stream '%s' (last sequence: %d)...", *streamName, streamLastSeq)

    // Get a list of all consumers on the stream.
    consumers := stream.ListConsumers(ctx)
    
    foundDurable := false
    for consumerInfo := range consumers.Info() {
	// Check if the consumer is durable.
	if consumerInfo.Config.Durable != "" {
	    foundDurable = true

	    // Calculate the difference (lag).
	    consumerLastSeq := consumerInfo.Delivered.Stream
	    lag := streamLastSeq - consumerLastSeq

	    log.Println("--------------------------------------------------")
	    log.Printf("Consumer: %s", consumerInfo.Config.Durable)
	    log.Printf("  Stream Last Seq: %d", streamLastSeq)
	    log.Printf("  Consumer Last Seq: %d", consumerLastSeq)
	    log.Printf("  Difference (Lag): %d", lag)
	}
    }

    if !foundDurable {
	log.Println("No durable consumers found on this stream.")
    }

    log.Println("--------------------------------------------------")
    log.Println("Exiting...")
}