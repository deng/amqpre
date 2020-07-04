package amqpre

import (
	"fmt"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

func TestRabbitMq(t *testing.T) {
	url := "amqp://admin:admin@172.17.12.3:5672/"
	limit := 5
	rmq, err := NewServer(url, limit)
	if err != nil {
		t.Fatalf("test NewServer err : %v ", err)
	}

	queueName := ""
	consumer, err := NewConsumer(rmq,
		ExParams{
			Name:    "sign",
			Kind:    "direct",
			Durable: true,
		},
		QueueParams{
			Name:    queueName,
			Durable: true,
			Binds:   []BindParams{{Key: queueName}},
		},
		ConsumerParams{
			Tag: "momo-server",
		},
	)
	if err != nil {
		t.Fatalf("test NewConsumer err : %v ", err)
	}

	publisher, err := NewPublisher(rmq,
		ExParams{
			Name:    "sign",
			Kind:    "direct",
			Durable: true,
		},
		PublishParams{
			RoutingKey: queueName,
		})
	if err != nil {
		t.Fatalf("test NewPublisher err : %v ", err)
	}

	//go func() {
	for i := 0; i < 10; i++ {
		// publisher.send 或者使用事务性的 publisher.sendTx
		if err = publisher.Send([]byte(fmt.Sprintf("test %d", i))); err != nil {
			t.Logf("publish send err : %v", err)
		}
	}
	//}()

	go func() {
		consumer.Consume(func(m *amqp.Delivery) {
			if m.Body == nil {
				return
			}

			t.Log(fmt.Sprintf("consumer process %s", string(m.Body)))
			m.Ack(false)
		})
	}()

	//go func() {
	time.Sleep(time.Second * time.Duration(30))

	err = publisher.Close()
	if err != nil {
		t.Logf("test Publisher Close err : %v ", err)
	}

	err = consumer.Close()
	if err != nil {
		t.Logf("test Publisher Close err : %v ", err)
	}

	rmq.Shutdown("reasons")
	//}()
}
