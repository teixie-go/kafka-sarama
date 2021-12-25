package kafka_sarama

import (
	"strings"

	"github.com/Shopify/sarama"
)

var (
	_ Producer = (*syncProducer)(nil)
	_ Producer = (*asyncProducer)(nil)
)

type ProducerConfig struct {
	// kafka地址，英文","分隔
	Addrs string `yaml:"addrs" json:"addrs"`
}

type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) error
	Close() error
}

func NewProducerMessage(topic, key string, value []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}

//------------------------------------------------------------------------------

type asyncProducer struct {
	producer sarama.AsyncProducer
}

func (a *asyncProducer) SendMessage(msg *sarama.ProducerMessage) error {
	a.producer.Input() <- msg
	select {
	case err := <-a.producer.Errors():
		logger.Errorf("Produced message failure: msg=%+v err=%v", *msg, err)
	default:
	}
	return nil
}

func (a *asyncProducer) Close() error {
	return a.producer.Close()
}

func NewAsyncProducer(cfg ProducerConfig) (Producer, error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(cfg.Addrs, ","), config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &asyncProducer{producer: producer}, nil
}

//------------------------------------------------------------------------------

type syncProducer struct {
	producer sarama.SyncProducer
}

func (s *syncProducer) SendMessage(msg *sarama.ProducerMessage) error {
	_, _, err := s.producer.SendMessage(msg)
	return err
}

func (s *syncProducer) Close() error {
	return s.producer.Close()
}

func NewSyncProducer(cfg ProducerConfig) (Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	client, err := sarama.NewClient(strings.Split(cfg.Addrs, ","), config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &syncProducer{producer: producer}, nil
}
