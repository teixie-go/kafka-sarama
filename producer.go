package kafka_sarama

import (
	"github.com/Shopify/sarama"
)

var (
	_ Producer = (*syncProducer)(nil)
	_ Producer = (*asyncProducer)(nil)
)

type (
	producer struct {
		brokers []string
		config  *sarama.Config
	}

	ProducerOption func(*producer)
)

func WithProducerBrokers(brokers []string) ProducerOption {
	return func(p *producer) {
		p.brokers = brokers
	}
}

func WithProducerConfig(config *sarama.Config) ProducerOption {
	return func(p *producer) {
		p.config = config
	}
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

func NewAsyncProducer(opts ...ProducerOption) (Producer, error) {
	p := &producer{
		config: sarama.NewConfig(),
	}
	for _, o := range opts {
		o(p)
	}

	client, err := sarama.NewClient(p.brokers, p.config)
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

func NewSyncProducer(opts ...ProducerOption) (Producer, error) {
	p := &producer{
		config: sarama.NewConfig(),
	}
	for _, o := range opts {
		o(p)
	}

	p.config.Producer.Return.Successes = true
	client, err := sarama.NewClient(p.brokers, p.config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &syncProducer{producer: producer}, nil
}
