package producer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type asyncProducer struct {
	emitter sarama.AsyncProducer
	configs *sarama.Config
}

func NewAsyncProducer(configs *sarama.Config, brokers []string) (*asyncProducer, error) {
	if configs == nil {
		configs = sarama.NewConfig()
	}

	configs.Producer.RequiredAcks = sarama.WaitForLocal
	configs.Producer.Flush.Frequency = 500 * time.Millisecond

	p, err := sarama.NewAsyncProducer(brokers, configs)
	if err != nil {
		return nil, err
	}

	go p.Errors()

	return &asyncProducer{
		emitter: p,
		configs: configs,
	}, nil
}

func (p *asyncProducer) SendMessage(topic string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	p.emitter.Input() <- msg
	return nil
}

func (p *asyncProducer) SendKeyedMessage(topic string, key string, value interface{}) error {
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

	p.emitter.Input() <- msg

	return nil
}

func (p *asyncProducer) Close() error {
	return p.emitter.Close()
}

func (p *asyncProducer) Errors() {
	for err := range p.emitter.Errors() {
		fmt.Printf("error while emitting: message: %v, error: %v", err.Msg, err)
		// TODO: Handle this error, by retrying to produce it again?
	}
}
