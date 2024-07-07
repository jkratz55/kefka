# Kefka

Kefka is an opinionated wrapper around the official Confluent Kafka Go client. Kefka offers a higher level API for consuming and producing messages along with offering common functionality such as retries and publishing failed messages to a dead letter topic. The motivation for Kefka came from writing the same boilerplate code over and over where I had many microservices utilizing Kafka. Kefka attempts to abstract away the lower level details of working with the Confluent Kafka Go API and allow developers to focus instead on processing and handling message as part of their business/application logic.

The name Kefka comes from Kefka Palazzo, the main antagonist of the critically acclaimed video game Final Fantasy VI.

<p align="center">
<img src=".resources/80-ffvi37_181.png">
</p>

Kefka offers four main types, each with their own purpose.

* Consumer: Used for Consumer messages from Kafka as a member of a consumer group. This is the most typical use case when consuming messages from Kafka.
* Reader: Used for consumer messages from Kafka but **not** as a member of a consumer group. This is intended more for use cases where you need to view the contents of a topic/partition but aren't consumer and persisting or processing the messages.
* Producer: Used for producing/publishing messages to Kafka
* AdminClient: Provides helper methods to work in tangent with Reader to fetch topics and partitions, offsets, watermarks, etc.

## Handler

At the heart of consuming/reading messages with Kefka is the Handler interface. The Handler interface defines a single method Handle which accepts a message and returns an error. The implementation of Handler supplied to the Consumer or Reader types is responsible for processing and handling messages as they are polled from Kafka. The Handler implementation should perform any validation, business logic, persistence, etc. and then either return nil, if there was no error, or return a non-nil error if an error occurred processing the message. Regardless, the Consumer will store the offsets and continue onwards to the next message but for proper instrumentation and logging it's important to correctly bubble up errors.

```go
type MyHandler struct {
	db *pgx.Conn
}

func (mh *MyHandler) Handle(msg *kafka.Message) error {
	// todo: store message in DB
	return nil
}
```

Because Handler is an interface it is trivial to wrap Handler and create middlewares. Kefka provides two Handler middlewares out of the box, but you can easily create your own. Kefka has the following built in ready to leverage:

1. Retry - Will handle retrying a Handler that returns a retryable error up to the maximum attempts.
2. DeadLetter - Will publish a message to a configured dead letter topic when the Handler returns an error

You can chain as many handlers/middlewares as you need/want but the ordering of them can matter depending on the middleware and use case. A typical chain may look something like this:

Consumer --> Dead Letter Handler --> Retry Handler --> Handler

In the example above, should the handler actually processing the message return an error the retry handler will invoke it again if the returned error is retryable. If the error isn't retryable or all attempts are exhausted, then the dead letter handler would publish the message to the configured dead letter topic.

## Consumer vs Reader

The Consumer and Reader types similar on the surface but have significant differences and different use cases. A Consumer is a member of a Consumer Group in Kafka and works with other members of the consumer group coordinated by the Kafka brokers. Kafka will assign out partitions based on the members in a consumer group, but at any given time, no more than one consumer will be consuming the same partition. The consumers commit offsets of the messages they've consumed back to the brokers to track the position for which the consumer has processed for each topic/partition.

The Reader type does not leverage consumer groups and directly assigns the configured topic partitions to itself, and reads the messages from those topics partitions. The Reader type never commits the offsets back to the brokers. The Reader type has two primary use cases:

1. View/Retrieve the contents of a set of topic partitions
2. Applications/Microservices that cache the contents of topic in memory and re-reads the entire contents on startup (generally not a good idea)

# Producer

The Producer in the underlying Confluent Kafka Go library that Kefka wraps is asynchronous. However, Kefka offers both a synchronous and asynchronous API. When using the synchronous API Kefka will handle the delivery report and bubble up any errors that occurred. When using the asynchronous API you must supply a non-nil `chan kafka.Event` and read the delivery reports from the channel if you care about error and ensuring the message was successfully produced. If you provide a nil channel than Kefka operates like the underlying Confluent Kafka client, and it is a fire and forget operation.

The Producer has two ways to produce messages.

1. Use the Produce or ProduceAndWait methods on the Producer type
2. Use the MessageBuilder 

The APIs provide the same functionality, so it comes down to personal preference. The MessageBuilder provides a more fluid API which some developers may prefer.

Producer

```go
err := producer.ProduceAndWait(&kafka.Message{
    TopicPartition: kafka.TopicPartition{
        Topic:     kefka.StringPtr("test"),
        Partition: kafka.PartitionAny,
    },
    Key:   []byte("mykey"),
    Value: []byte("myvalue"),
})
if err != nil {
// todo: handle error
}
```

MessageBuilder

```go
err := producer.M().
    Topic("test").
    Key("mykey").
    JSON(map[string]interface{}{
        "event": "CREATE_USER",
    }).
    SendAndWait()
if err != nil {
    // todo: handle error
}
```

_Note that Kefka will always set Partition to `PartitionAny`._

## Further Reading

To learn more about Kefka checkout the examples directory in this repository.