## 开始使用
### 1.创建消费者
```
func DemoMessageHandler(message *sarama.ConsumerMessage) error {
	fmt.Printf("message: %v\n", string(message.Value))
	return nil
}

func main() {
	config := kafka_sarama.ConsumerGroupConfig{
		Addrs:   "127.0.0.1:9092",
		GroupId: "test",
		Topics:  "test",
	}
	
	// 实际使用时，这段代码通常需要放在goroutine中异步执行
	err := kafka_sarama.NewConsumerGroupClient(config, kafka_sarama.NewConsumer(DemoMessageHandler))
	if err != nil {
	    panic(err)
	}
}
```
### 2.创建生产者
```
func main() {
    config := kafka_sarama.ProducerConfig{
        Addrs: "127.0.0.1:9092",
    }
    
    producer, err := kafka_sarama.NewSyncProducer(config)
    if err != nil {
    	panic(err)
    }
    defer producer.Close()
    
    // value可以由具体应用来自定义协议，并类似这样实现便捷创建与解析
    // value, err := xxxEncoder.Encode(struct{})
    // if err != nil {
    //    return err
    // }
    err = producer.SendMessage(kafka_sarama.NewProducerMessage("test", []byte("test"), []byte(value)))
    if err != nil {
    	fmt.Printf("err: %v\n", err)
    }
}
```