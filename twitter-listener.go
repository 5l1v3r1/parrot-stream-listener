package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"

	anaconda "github.com/ChimeraCoder/anaconda"
	sarama "github.com/Shopify/sarama"
)

func main() {
	consumerKey := flag.String("tck", "", "Twitter Consumer Key")
	consumerSecret := flag.String("tcs", "", "Twitter Consumer Secret")
	accessToken := flag.String("tat", "", "Twitter Access Token")
	accessTokenSecret := flag.String("tats", "", "Twitter Access Token Secret")
	tracks := flag.String("tracks", "trump", "Tracks to listen")
	lang := flag.String("lang", "en", "Tweets language")
	kafkaAddress := flag.String("kaddrs", "localhost:9092", "Kafka Endpoint Address")
	kafkaTopic := flag.String("ktopic", "test", "Kafka Topic")
	flag.Parse()

	if *consumerKey == "" || *consumerSecret == "" || *accessToken == "" || *accessTokenSecret == "" {
		println("Please specify twitter keys and access tokens")
		os.Exit(-1)
	}

	// Configuring Twitter API and parameters
	anaconda.SetConsumerKey(*consumerKey)
	anaconda.SetConsumerSecret(*consumerSecret)
	api := anaconda.NewTwitterApi(*accessToken, *accessTokenSecret)
	args := url.Values{}
	args.Set("track", *tracks)
	args.Set("language", *lang)

	// Setting up Kafka Producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	addrs := []string{*kafkaAddress}
	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	// Listening to Public Stream
	stream := api.PublicStreamFilter(args)
	var limitCount int64
	for item := range stream.C {
		switch resp := item.(type) {
		case anaconda.Tweet:
			b, err := json.Marshal(resp)
			if err != nil {
				fmt.Println(err)
				return
			}
			msg := &sarama.ProducerMessage{
				Topic: *kafkaTopic,
				Value: sarama.StringEncoder(b),
			}
			_, _, err = producer.SendMessage(msg)
			if err != nil {
				panic(err)
			}
		case anaconda.LimitNotice:
			limitCount += resp.Track
			fmt.Println("Undelivered tweets by API:", limitCount)
		default:
			fmt.Println("Not sure what happened here, check it out:", resp)
		}
	}
}
