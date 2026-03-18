package cmd

import (
	"encoding/json"
	"fmt"

	bunnyProducer "github.com/patrickjmcd/bunny/producer"
	"github.com/patrickjmcd/go-tracing"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// fillCmd represents the fill command
var fillCmd = &cobra.Command{
	Use:   "fill",
	Short: "Write messages to a rabbit queue",
	Long:  `Write any number of messages to a rabbit queue, typically used for testing`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("fill called")
		fmt.Println("exchange type is ", viper.GetString("to-exchange-type"))

		valueSeralizer, err := bunnyProducer.NewJsonSerializer[map[string]interface{}]()
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create value serializer")
		}
		// Setup open telemetry tracing
		tp, tpErr := tracing.OpenTelemetryTraceProvider(serviceName)
		if tpErr != nil {
			log.Fatal().Err(tpErr).Msg("error creating trace provider")
		}
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
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

		for i := 0; i < viper.GetInt("count"); i++ {
			var val map[string]interface{}
			err := json.Unmarshal([]byte(viper.GetString("message")), &val)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to unmarshal message")
			}
			fmt.Printf("Writing message %d - %+v\n", i, val)
			producer.ProduceMessage(cmd.Context(), fmt.Sprintf("%d", i), val, "")
		}
		producer.Close()
	},
}

func init() {
	rootCmd.AddCommand(fillCmd)

	fillCmd.Flags().IntP("count", "n", 1, "The number of messages to write to the queue")
	viper.BindPFlag("count", fillCmd.Flags().Lookup("count"))
	viper.SetDefault("count", 1)

	fillCmd.Flags().StringP("message", "m", `{"msg": "Hello, World!"}`, "The JSON message to write to the queue")
	viper.BindPFlag("message", fillCmd.Flags().Lookup("message"))
	viper.SetDefault("message", `{"msg": "Hello, World!"}`)
}
