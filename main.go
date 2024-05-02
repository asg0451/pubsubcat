package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/jessevdk/go-flags"
	"go.coldcutz.net/go-stuff/utils"
)

type Options struct {
	ProjectID          string `short:"p" long:"project-id" description:"GCP Project ID" env:"PROJECT_ID" required:"true"`
	Topic              string `short:"t" long:"topic" description:"Pub/Sub Topic" env:"TOPIC"`
	Subscription       string `short:"s" long:"subscription" description:"Pub/Sub Subscription" env:"SUBSCRIPTION"`
	CreateSubscription bool   `short:"c" long:"create-subscription" description:"Create subscription if it does not exist"`
}

func main() {
	ctx, done, log, err := utils.StdSetup()
	if err != nil {
		panic(err)
	}
	defer done()

	if err := run(ctx, log); err != nil {
		log.Error("failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	var opts Options
	if _, err := flags.Parse(&opts); err != nil {
		return err
	}

	client, err := pubsub.NewClient(ctx, opts.ProjectID)
	if err != nil {
		return err
	}
	defer client.Close()

	if opts.Subscription == "" {
		return fmt.Errorf("subscription is required to consume")
	}
	sub := client.Subscription(opts.Subscription)

	if opts.CreateSubscription {
		ok, err := sub.Exists(ctx)
		if err != nil {
			return err
		}
		if !ok {
			if opts.Topic == "" {
				return fmt.Errorf("topic is required to create subscription")
			}
			_, err := client.CreateSubscription(ctx, opts.Subscription, pubsub.SubscriptionConfig{
				Topic: client.Topic(opts.Topic),
			})
			if err != nil {
				return err
			}
			log.Info("created subscription", "subscription", opts.Subscription, "topic", opts.Topic)
		}
	}

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		defer m.Ack()
		fmt.Println(string(m.Data))
	})
	if err != nil {
		return err
	}

	return nil
}
