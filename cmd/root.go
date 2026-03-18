package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile     string
	serviceName = "draino"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "draino",
	Short: "Move rabbitmq messages around, or drain a queue",
	Long:  `Move rabbitmq messages from one exchange/queue to another, or drain a queue entirely.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		rootInitConfig()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func rootInitConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".draino")
	}

	viper.SetEnvPrefix(serviceName)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
	fmt.Println("config read in")
}

func init() {
	// cobra.OnInitialize(rootInitConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.draino.yaml)")

	rootCmd.PersistentFlags().StringP("from-connection-string", "f", "", "The connection string for the rabbitmq server to move messages from")
	viper.BindPFlag("from-connection-string", rootCmd.PersistentFlags().Lookup("from-connection-string"))
	viper.SetDefault("from-connection-string", "amqp://guest:guest@localhost:5672/")

	rootCmd.PersistentFlags().StringP("from-exchange-name", "e", "", "The name of the exchange to move messages from")
	viper.BindPFlag("from-exchange-name", rootCmd.PersistentFlags().Lookup("from-exchange-name"))
	viper.SetDefault("from-exchange-name", "test_exchange")

	rootCmd.PersistentFlags().StringP("from-exchange-type", "t", "direct", "The type of exchange to move messages from")
	viper.BindPFlag("from-exchange-type", rootCmd.PersistentFlags().Lookup("from-exchange-type"))
	viper.SetDefault("from-exchange-type", "direct")

	rootCmd.PersistentFlags().BoolP("from-exchange-autodelete", "a", false, "Whether or not the exchange should be auto-deleted")
	viper.BindPFlag("from-exchange-autodelete", rootCmd.PersistentFlags().Lookup("from-exchange-autodelete"))
	viper.SetDefault("from-exchange-autodelete", false)

	rootCmd.PersistentFlags().BoolP("from-exchange-durable", "d", true, "Whether or not the exchange should be durable")
	viper.BindPFlag("from-exchange-durable", rootCmd.PersistentFlags().Lookup("from-exchange-durable"))
	viper.SetDefault("from-exchange-durable", false)

	rootCmd.PersistentFlags().StringP("from-queue-name", "q", "", "The name of the queue to move messages from")
	viper.BindPFlag("from-queue-name", rootCmd.PersistentFlags().Lookup("from-queue-name"))
	viper.SetDefault("from-queue-name", "test_queue")

	rootCmd.PersistentFlags().BoolP("from-queue-autodelete", "A", false, "Whether or not the queue should be auto-deleted")
	viper.BindPFlag("from-queue-autodelete", rootCmd.PersistentFlags().Lookup("from-queue-autodelete"))
	viper.SetDefault("from-queue-autodelete", true)

	rootCmd.PersistentFlags().BoolP("from-queue-durable", "D", true, "Whether or not the queue should be durable")
	viper.BindPFlag("from-queue-durable", rootCmd.PersistentFlags().Lookup("from-queue-durable"))
	viper.SetDefault("from-queue-durable", false)

	rootCmd.PersistentFlags().String("to-connection-string", "", "The connection string for the rabbitmq destination")
	viper.BindPFlag("to-connection-string", rootCmd.PersistentFlags().Lookup("to-connection-string"))
	viper.SetDefault("to-connection-string", "amqp://guest:guest@localhost:5672/")

	rootCmd.PersistentFlags().String("to-exchange-name", "test_exchange", "The exchange to write messages to")
	viper.BindPFlag("to-exchange-name", rootCmd.PersistentFlags().Lookup("to-exchange-name"))
	viper.SetDefault("to-exchange-name", "test_exchange")

	rootCmd.PersistentFlags().String("to-exchange-type", "direct", "The exchange type to write messages to")
	viper.BindPFlag("to-exchange-type", rootCmd.PersistentFlags().Lookup("to-exchange-type"))
	viper.SetDefault("to-exchange-type", "direct")

	rootCmd.PersistentFlags().Bool("to-exchange-autodelete", false, "Delete the exchange when the last queue is unbound from it")
	viper.BindPFlag("to-exchange-autodelete", rootCmd.PersistentFlags().Lookup("to-exchange-autodelete"))
	viper.SetDefault("to-exchange-autodelete", false)

	rootCmd.PersistentFlags().Bool("to-exchange-durable", false, "Persist the exchange to disk")
	viper.BindPFlag("to-exchange-durable", rootCmd.PersistentFlags().Lookup("to-exchange-durable"))
	viper.SetDefault("to-exchange-durable", false)

	rootCmd.PersistentFlags().String("to-queue-name", "test_queue", "The queue to write messages to")
	viper.BindPFlag("to-queue-name", rootCmd.PersistentFlags().Lookup("to-queue-name"))
	viper.SetDefault("to-queue-name", "test_queue")

	rootCmd.PersistentFlags().Bool("to-queue-autodelete", true, "Delete the queue when the last consumer unsubscribes")
	viper.BindPFlag("to-queue-autodelete", rootCmd.PersistentFlags().Lookup("to-queue-autodelete"))
	viper.SetDefault("to-queue-autodelete", true)

	rootCmd.PersistentFlags().Bool("to-queue-durable", false, "Persist the queue to disk")
	viper.BindPFlag("to-queue-durable", rootCmd.PersistentFlags().Lookup("to-queue-durable"))
	viper.SetDefault("to-queue-durable", false)

	rootCmd.PersistentFlags().StringP("from-topic", "T", "", "The topic to filter messages on")
	viper.BindPFlag("from-topic", rootCmd.PersistentFlags().Lookup("from-topic"))
	viper.SetDefault("from-topic", "")

	rootCmd.PersistentFlags().String("to-topic", "", "The topic to produce messages on")
	viper.BindPFlag("to-topic", rootCmd.PersistentFlags().Lookup("to-topic"))
	viper.SetDefault("to-topic", "")
}
