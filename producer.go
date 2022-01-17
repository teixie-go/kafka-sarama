package kafka_sarama

import (
	"github.com/Shopify/sarama"
)

var (
	_ Producer = (*syncProducer)(nil)
	_ Producer = (*asyncProducer)(nil)
)

type ProducerConfig struct {
	Brokers []string
	Config  *sarama.Config
}

type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) error
	Close() error
}

func NewProducerMessage(topic string, key, value []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
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
	client, err := sarama.NewClient(cfg.Brokers, cfg.Config)
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
	cfg.Config.Producer.Return.Successes = true
	client, err := sarama.NewClient(cfg.Brokers, cfg.Config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &syncProducer{producer: producer}, nil
}
