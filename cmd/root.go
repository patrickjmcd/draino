/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
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
}

func init() {
	cobra.OnInitialize(rootInitConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.draino.yaml)")

	rootCmd.PersistentFlags().StringP("fromConnectionString", "f", "", "The connection string for the rabbitmq server to move messages from")
	viper.BindPFlag("fromConnectionString", rootCmd.PersistentFlags().Lookup("fromConnectionString"))
	viper.SetDefault("fromConnectionString", "amqp://guest:guest@localhost:5672/")

	rootCmd.PersistentFlags().StringP("fromExchangeName", "e", "", "The name of the exchange to move messages from")
	viper.BindPFlag("fromExchangeName", rootCmd.PersistentFlags().Lookup("fromExchangeName"))
	viper.SetDefault("fromExchangeName", "test_exchange")

	rootCmd.PersistentFlags().StringP("fromExchangeType", "t", "direct", "The type of exchange to move messages from")
	viper.BindPFlag("fromExchangeType", rootCmd.PersistentFlags().Lookup("fromExchangeType"))
	viper.SetDefault("fromExchangeType", "direct")

	rootCmd.PersistentFlags().BoolP("fromExchangeAutoDelete", "a", false, "Whether or not the exchange should be auto-deleted")
	viper.BindPFlag("fromExchangeAutoDelete", rootCmd.PersistentFlags().Lookup("fromExchangeAutoDelete"))
	viper.SetDefault("fromExchangeAutoDelete", false)

	rootCmd.PersistentFlags().BoolP("fromExchangeDurable", "d", true, "Whether or not the exchange should be durable")
	viper.BindPFlag("fromExchangeDurable", rootCmd.PersistentFlags().Lookup("fromExchangeDurable"))
	viper.SetDefault("fromExchangeDurable", false)

	rootCmd.PersistentFlags().StringP("fromQueueName", "q", "", "The name of the queue to move messages from")
	viper.BindPFlag("fromQueueName", rootCmd.PersistentFlags().Lookup("fromQueueName"))
	viper.SetDefault("fromQueueName", "test_queue")

	rootCmd.PersistentFlags().BoolP("fromQueueAutoDelete", "A", false, "Whether or not the queue should be auto-deleted")
	viper.BindPFlag("fromQueueAutoDelete", rootCmd.PersistentFlags().Lookup("fromQueueAutoDelete"))
	viper.SetDefault("fromQueueAutoDelete", true)

	rootCmd.PersistentFlags().BoolP("fromQueueDurable", "D", true, "Whether or not the queue should be durable")
	viper.BindPFlag("fromQueueDurable", rootCmd.PersistentFlags().Lookup("fromQueueDurable"))
	viper.SetDefault("fromQueueDurable", false)

	rootCmd.PersistentFlags().StringP("topic", "T", "", "The topic to filter messages on")
	viper.BindPFlag("topic", rootCmd.PersistentFlags().Lookup("topic"))
	viper.SetDefault("topic", "")
}
