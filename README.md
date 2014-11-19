Misakai.Kafka
=============

High-Performance C# client for Apache Kafka. This client was designed not to support all possibile features, but to serve as a minimalistic and lightweight Kafka client for long-running producers/consumers. The code was based on James Roland's KafkaNet implementation, trimmed down and optimized significantly.

Examples
-----------
##### Producer
```sh
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));
var router = new BrokerRouter(options);
var client = new Producer(router);

client.SendMessageAsync("TestHarness", new[] { new Message { Value = message } }).Wait();

using (client) { }
```
##### Consumer
```sh
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));
var router = new BrokerRouter(options);
var consumer = new Consumer(new ConsumerOptions { Topic = "TestHarness", Router = router });

//Consume returns a blocking IEnumerable (ie: never ending stream)
foreach (var message in consumer.Consume())
{
    Console.WriteLine("Response: P{0},O{1} : {2}", 
        message.Meta.PartitionId, message.Meta.Offset, message.Value);  
}
```

Pieces of the Puzzle
-----------
##### Protocol
The protocol has been divided up into concrete classes for each request/response pair.  Each class knows how to encode and decode itself into/from their appropriate Kafka protocol byte array.  One benefit of this is that it allows for a nice generic send method on the KafkaConnection.

##### KafkaConnection
Provides async methods on a persistent connection to a kafka broker (server).  The send method uses the TcpClient send async function and the read stream has a dedicated thread which uses the correlation Id to match send responses to the correct request.

##### BrokerRouter
Provides metadata based routing of messages to the correct Kafka partition.  This class also manages the multiple KafkaConnections for each Kafka server returned by the broker section in the metadata response.  Routing logic is provided by the IPartitionSelector.

##### IPartitionSelector
Provides the logic for routing which partition the BrokerRouter should choose.  The default selector is the DefaultPartitionSelector which will use round robin partition selection if the key property on the message is null and a mod/hash of the key value if present.

##### Producer
Provides a higher level class which uses the combination of the BrokerRouter and KafkaConnection to send batches of messages to a Kafka broker.

##### Consumer
Provides a higher level class which will consumer messages from a whitelist of partitions from a single topic.  The consumption mechanism is a blocking IEnumerable of messages.  If no whitelist is provided then all partitions will be consumed creating one KafkaConnection for each partition leader.
