package consumer

import (
	"fmt"

	"github.com/bsm/sarama-cluster"
)

type consumer struct {
	consumer *cluster.Consumer
	configs  *Config
}

type Config struct {
	Brokers       []string
	GroupID       string
	Topics        []string
	ClusterConfig *cluster.Config
	ConsumerMode  cluster.ConsumerMode
}

func NewConsumer(configs *Config) (*consumer, error) {
	if configs.ClusterConfig == nil {
		configs.ClusterConfig = cluster.NewConfig()
	}

	configs.ClusterConfig.Consumer.Return.Errors = true
	configs.ClusterConfig.Group.Return.Notifications = true
	if configs.ConsumerMode == cluster.ConsumerModePartitions {
		configs.ClusterConfig.Group.Mode = cluster.ConsumerModePartitions
	}

	c, err := cluster.NewConsumer(configs.Brokers, configs.GroupID, configs.Topics, configs.ClusterConfig)
	if err != nil {
		return nil, err
	}

	consumer := &consumer{
		consumer: c,
		configs:  configs,
	}

	go func() {
		for er := range consumer.consumer.Errors() {
			// TODO: handle error
			fmt.Printf("error while consuming: %v", er)
		}
	}()

	go func() {
		for not := range consumer.consumer.Notifications() {
			fmt.Printf("cluster status: %v", not)
		}
	}()

	return consumer, nil
}

func (c *consumer) Consume() {
	if c.configs.ConsumerMode == cluster.ConsumerModePartitions {
		go c.consumedPartitions()
		return
	}

	go c.consumeMultiplexed()
}

func (c *consumer) Close() error {
	return c.consumer.Close()
}

func (c *consumer) consumeMultiplexed() {
	for msg := range c.consumer.Messages() {
		fmt.Printf("%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
		c.consumer.MarkOffset(msg, "")
	}
}

func (c *consumer) consumedPartitions() {
	for partition := range c.consumer.Partitions() {
		go func(p cluster.PartitionConsumer) {
			for msg := range p.Messages() {
				fmt.Println("%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				c.consumer.MarkOffset(msg, "")
			}
		}(partition)
	}
}
