/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	bunnyConsumer "github.com/patrickjmcd/bunny/consumer"
	"github.com/patrickjmcd/go-tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"
)

// drainCmd represents the drain command
var drainCmd = &cobra.Command{
	Use:   "drain",
	Short: "Drain the contents of a rabbit queue, optionally printing them out first",
	Long:  `Connect to a rabbit exchange/queue and drain the contents of the queue. Optionally print the contents while draining.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("drain called")
		destructive := viper.GetBool("destructive")
		fmt.Println("destructive: ", destructive)
		tp, tpErr := tracing.OpenTelemetryTraceProvider(serviceName)
		if tpErr != nil {
			log.Fatal().Err(tpErr).Msg("error creating trace provider")
		}
		otel.SetTracerProvider(tp)
		propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
		otel.SetTextMapPropagator(propagator) // set the propagator globally

		processingDelay, _ := time.ParseDuration(viper.GetString("processing_delay_ms") + "ms")
		valueDeserializer, _ := bunnyConsumer.NewJsonValueDeserializer[map[string]interface{}]()

		messageTicker := make(chan bool, 1)

		handler := &DrainHandler{messageTicker: messageTicker}
		errorHandler := &DrainFakeErrorHandler{destructive: destructive}

		consumer, err := bunnyConsumer.NewRabbitConsumer(
			bunnyConsumer.WithConnectionString(viper.GetString("fromConnectionString")),
			bunnyConsumer.WithExchangeName(viper.GetString("fromExchangeName")),
			bunnyConsumer.WithExchangeType(viper.GetString("fromExchangeType")),
			bunnyConsumer.WithExchangeAutoDelete(viper.GetBool("fromExchangeAutoDelete")),
			bunnyConsumer.WithExchangeDurable(viper.GetBool("fromExchangeDurable")),
			bunnyConsumer.WithQueueName(viper.GetString("fromQueueName")),
			bunnyConsumer.WithQueueAutoDelete(viper.GetBool("fromQueueAutoDelete")),
			bunnyConsumer.WithQueueDurable(viper.GetBool("fromQueueDurable")),
			bunnyConsumer.WithTopic(viper.GetString("topic")),
			bunnyConsumer.WithValueDeserializer(valueDeserializer),
			bunnyConsumer.WithProcessingDelay(processingDelay), bunnyConsumer.WithTracePropagator(propagator),
			bunnyConsumer.WithTracer(tp.Tracer(serviceName)),
			bunnyConsumer.WithMessageErrorHandler(errorHandler),
			bunnyConsumer.WithMessageHandler(handler),
			bunnyConsumer.WithConsumerAutoAck(false),
			bunnyConsumer.WithSuppressProcessingErrors(true),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("error creating consumer")
		}
		<-consumer.Ready

		ctx, cancel := context.WithCancel(context.Background())
		g, gctx := errgroup.WithContext(ctx)
		defer consumer.Close()

		g.Go(func() error {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
			select {
			case sig := <-sigs:
				log.Info().Msgf("received %s signal", sig)
				cancel()
			case <-gctx.Done():
				return gctx.Err()
			}
			return nil
		})

		g.Go(func() error {
			for {
				select {
				case <-messageTicker:
					continue
				case <-time.Tick(1 * time.Second):
					log.Info().Msg("no messages for 1 second, closing")
					cancel()
				case <-gctx.Done():
					return gctx.Err()
				}
			}
		})

		g.Go(func() error {
			return consumer.Run(gctx)
		})

		err = g.Wait()
		if err != nil {
			log.Error().Err(err).Msg("error running consumer")
		}
	},
}

type DrainHandler struct {
	messageTicker chan bool
}

func (p *DrainHandler) OnReceive(ctx context.Context, key, value interface{}) error {
	// throw an error here so that the error handler, which has access to ther raw message, can handle it
	v := value.(map[string]interface{})
	jsonString, err := json.Marshal(v)
	if err != nil {
		fmt.Println("error marshalling json", err)
	}
	fmt.Println(string(jsonString))
	p.messageTicker <- true
	return fmt.Errorf("error")
}

type DrainFakeErrorHandler struct {
	destructive bool
}

func (p *DrainFakeErrorHandler) OnError(ctx context.Context, raw *amqp.Delivery) error {
	if p.destructive {
		return raw.Ack(false)
	} else {
		return nil
	}
}

func init() {
	rootCmd.AddCommand(drainCmd)

	drainCmd.Flags().BoolP("destructive", "X", false, "Delete the messages after printing them")
	viper.BindPFlag("destructive", drainCmd.Flags().Lookup("destructive"))
	viper.SetDefault("destructive", false)

	viper.BindEnv("processing_delay_ms")
	viper.SetDefault("processing_delay_ms", 0)
}
