package kafkarouter

import "time"

type Message struct {
	Topic string
	// TODO: headers?
	Key       []byte
	Payload   []byte
	Timestamp time.Time
}
