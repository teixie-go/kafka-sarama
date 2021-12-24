package kafka_sarama

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var (
	// 默认kafka版本号
	DefaultVersion = "2.1.1"

	// 默认标准输出日志
	logger Logger = &StdLogger{logger: log.New(os.Stderr, "", log.LstdFlags)}
)

type ConsumerGroupConfig struct {
	// kafka版本
	Version string `yaml:"version" json:"version"`

	// Broker服务器地址列表
	Addrs string `yaml:"addrs" json:"addrs"`

	// 消费群组ID
	GroupId string `yaml:"group_id" json:"group_id"`

	// 消费主题列表
	Topics string `yaml:"topics" json:"topics"`
}

// 新建群组消费者客户端
func NewConsumerGroupClient(cfg ConsumerGroupConfig, consumer sarama.ConsumerGroupHandler) error {
	version, err := sarama.ParseKafkaVersion(resolveVersion(cfg.Version))
	if err != nil {
		return err
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	client, err := sarama.NewConsumerGroup(strings.Split(cfg.Addrs, ","), cfg.GroupId, config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, strings.Split(cfg.Topics, ","), consumer); err != nil {
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
	if err := client.Close(); err != nil {
		logger.Errorf("Error closing client: %v", err)
	}
	return err
}

// 获取kafka版本
func resolveVersion(ver string) string {
	if ver != "" {
		return ver
	}
	return DefaultVersion
}

//------------------------------------------------------------------------------

type MessageHandler func(message *sarama.ConsumerMessage) error

// Consumer represents a Sarama consumer group consumer
type consumer struct {
	messageHandler MessageHandler
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	logger.Info("Sarama consumer up and running...")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		err := c.messageHandler(message)
		if err != nil {
			logger.Errorf("Message claimed error: message=%+v, err=%v", *message, err)
		} else {
			session.MarkMessage(message, "")
		}
	}
	return nil
}

// NewConsumer 新建消费者实例
func NewConsumer(handler MessageHandler) sarama.ConsumerGroupHandler {
	return &consumer{messageHandler: handler}
}

//------------------------------------------------------------------------------

type Logger interface {
	Info(args ...interface{})
	Errorf(format string, args ...interface{})
}

type StdLogger struct {
	logger *log.Logger
}

func (l *StdLogger) Info(args ...interface{}) {
	l.logger.Println(args...)
}

func (l *StdLogger) Errorf(format string, args ...interface{}) {
	l.logger.Printf(format, args...)
}

func SetLogger(_logger Logger) {
	logger = _logger
}
