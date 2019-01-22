package producer

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type producer struct {
	emitter sarama.SyncProducer
	configs *sarama.Config
}

func NewProducer(configs *sarama.Config, brokers []string) (*producer, error) {
	if configs == nil {
		configs = sarama.NewConfig()
	}

	configs.Producer.RequiredAcks = sarama.WaitForAll
	configs.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(brokers, configs)
	if err != nil {
		return nil, err
	}

	return &producer{
		emitter: p,
		configs: configs,
	}, nil
}

func (p *producer) SendMessage(topic string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	part, off, err := p.emitter.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("messeage: %s, partiton: %v, offset: %v", msg.Value, part, off)
	return nil
}

func (p *producer) SendKeyedMessage(topic string, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	part, off, err := p.emitter.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("messeage: %s, partiton: %v, offset: %v", msg.Value, part, off)
	return nil
}

func (p *producer) Close() error {
	return p.emitter.Close()
}
