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
	bunnyProducer "github.com/patrickjmcd/bunny/producer"
	"github.com/patrickjmcd/go-tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"
)

// moveCmd represents the move command
var moveCmd = &cobra.Command{
	Use:   "move",
	Short: "Move the contents of one rabbit exchange/queue to another",
	Long:  `Connect to a rabbit exchange/queue and move the contents of the queue to another exchange/queue, optionally printing the contents while moving.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("move called")
		destructive := viper.GetBool("destructive-move")
		fmt.Println("destructive: ", destructive)
		tp, tpErr := tracing.OpenTelemetryTraceProvider(serviceName)
		if tpErr != nil {
			log.Fatal().Err(tpErr).Msg("error creating trace provider")
		}
		otel.SetTracerProvider(tp)
		propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
		otel.SetTextMapPropagator(propagator) // set the propagator globally

		valueSeralizer, err := bunnyProducer.NewJsonSerializer[map[string]interface{}]()
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create value serializer")
		}

		producer, err := bunnyProducer.NewRabbitProducer(
			bunnyProducer.WithConnectionString(viper.GetString("to-connection-string")),
			bunnyProducer.WithExchangeName(viper.GetString("to-exchange-name")),
			bunnyProducer.WithExchangeType(viper.GetString("to-exchange-type")),
			bunnyProducer.WithExchangeAutoDelete(viper.GetBool("to-exchange-autodelete")),
			bunnyProducer.WithExchangeDurable(viper.GetBool("to-exchange-durable")),
			bunnyProducer.WithQueueName(viper.GetString("to-queue-name")),
			bunnyProducer.WithQueueAutoDelete(viper.GetBool("to-queue-autodelete")),
			bunnyProducer.WithQueueDurable(viper.GetBool("to-queue-durable")),
			bunnyProducer.WithTopic(viper.GetString("to-topic")),
			bunnyProducer.WithValueSerializer(valueSeralizer),
			bunnyProducer.WithTracer(tp.Tracer(serviceName)),
			bunnyProducer.WithQueueNoBind(true),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create producer")
		}
		<-producer.Ready

		processingDelay, _ := time.ParseDuration(viper.GetString("processing_delay_ms") + "ms")
		valueDeserializer, _ := bunnyConsumer.NewJsonValueDeserializer[map[string]interface{}]()

		messageTicker := make(chan bool, 1)
		messageTimeout := time.Duration(viper.GetInt("timeout"))
		printMessages := viper.GetBool("print")

		handler := &MoveHandler{messageTicker: messageTicker, printMessages: printMessages, producer: producer}
		errorHandler := &MoveFakeErrorHandler{destructive: destructive}

		consumer, err := bunnyConsumer.NewRabbitConsumer(
			bunnyConsumer.WithConnectionString(viper.GetString("from-connection-string")),
			bunnyConsumer.WithExchangeName(viper.GetString("from-exchange-name")),
			bunnyConsumer.WithExchangeType(viper.GetString("from-exchange-type")),
			bunnyConsumer.WithExchangeAutoDelete(viper.GetBool("from-exchange-autodelete")),
			bunnyConsumer.WithExchangeDurable(viper.GetBool("from-exchange-durable")),
			bunnyConsumer.WithQueueName(viper.GetString("from-queue-name")),
			bunnyConsumer.WithQueueAutoDelete(viper.GetBool("from-queue-autodelete")),
			bunnyConsumer.WithQueueDurable(viper.GetBool("from-queue-durable")),
			bunnyConsumer.WithTopic(viper.GetString("from-topic")),
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
				case <-time.Tick(messageTimeout * time.Second):
					log.Info().Msgf("no messages for %d second, closing", viper.GetInt("timeout"))
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
			if err != context.Canceled {
				log.Error().Err(err).Msg("error running consumer")
			}
		}
		if viper.GetBool("count") {
			log.Info().Msgf("read %d messages", handler.counter)
		}
	},
}

type MoveHandler struct {
	messageTicker chan bool
	printMessages bool
	counter       int
	producer      *bunnyProducer.RabbitProducer
}

func (m *MoveHandler) OnReceive(ctx context.Context, key, value interface{}) error {
	// throw an error here so that the error handler, which has access to ther raw message, can handle it
	v := value.(map[string]interface{})
	jsonString, err := json.Marshal(v)
	if err != nil {
		fmt.Println("error marshalling json", err)
	}
	fmt.Println(string(jsonString))
	m.messageTicker <- true

	err = m.producer.ProduceMessage(ctx, key.(string), v, "")
	if err != nil {
		log.Error().Err(err).Msg("failed to produce message")
		return err
	}

	m.counter++
	return fmt.Errorf("error")
}

func (m *MoveHandler) PrintCount() {
	log.Info().Msgf("read %d messages", m.counter)
}

type MoveFakeErrorHandler struct {
	destructive bool
}

func (m *MoveFakeErrorHandler) OnError(ctx context.Context, raw *amqp.Delivery) error {
	if m.destructive {
		return raw.Ack(false)
	} else {
		return nil
	}
}

func init() {
	rootCmd.AddCommand(moveCmd)

	viper.BindEnv("processing_delay_ms")
	viper.SetDefault("processing_delay_ms", 0)

	moveCmd.Flags().Int("timeout", 1, "The amount of time (in seconds) to wait for messages before closing the consumer")
	viper.BindPFlag("timeout", moveCmd.Flags().Lookup("timeout"))
	viper.SetDefault("timeout", 1)

	moveCmd.Flags().BoolP("print", "p", false, "Print the messages as they are moved")
	viper.BindPFlag("print", moveCmd.Flags().Lookup("print"))
	viper.SetDefault("print", false)

	moveCmd.Flags().BoolP("destructive", "X", false, "Delete the messages after printing them")
	viper.BindPFlag("destructive-move", moveCmd.Flags().Lookup("destructive"))
	viper.SetDefault("destructive-move", false)
}
