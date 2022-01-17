package kafka_sarama

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var (
	// 默认标准输出日志
	logger Logger = &stdLogger{logger: log.New(os.Stderr, "", log.LstdFlags)}
)

type ConsumerGroupConfig struct {
	Brokers  []string
	Topics   []string
	GroupId  string
	Config   *sarama.Config
	Consumer sarama.ConsumerGroupHandler
}

// NewConsumerGroupClient 新建群组消费者客户端
func NewConsumerGroupClient(cfg ConsumerGroupConfig) error {
	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupId, cfg.Config)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Errorf("Error closing consumer client: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, cfg.Topics, cfg.Consumer); err != nil {
				logger.Errorf("Error from consumer: %v", err)
				time.Sleep(5 * time.Second)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		logger.Info("terminating: context cancelled")
	case <-sigterm:
		logger.Info("sig terminatting")
	}

	cancel()
	wg.Wait()
	return ctx.Err()
}

//------------------------------------------------------------------------------

type Claimer interface {
	Claim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}

// Consumer represents a Sarama consumer group consumer
type consumer struct {
	claimer Claimer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	logger.Info("Sarama consumer up and running...")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	logger.Info("Sarama consumer cleanup...")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	return c.claimer.Claim(session, claim)
}

// NewConsumer 新建消费者实例
func NewConsumer(claimer Claimer) sarama.ConsumerGroupHandler {
	return &consumer{claimer: claimer}
}

//------------------------------------------------------------------------------

type Logger interface {
	Info(args ...interface{})
	Errorf(format string, args ...interface{})
}

type stdLogger struct {
	logger *log.Logger
}

func (s *stdLogger) Info(args ...interface{}) {
	s.logger.Println(args...)
}

func (s *stdLogger) Errorf(format string, args ...interface{}) {
	s.logger.Printf(format, args...)
}

func SetLogger(l Logger) {
	logger = l
}
