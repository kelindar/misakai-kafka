Misakai.Kafka
=============

High-Performance C# client for Apache Kafka. This client was designed not to support all possibile features, but to serve as a minimalistic and lightweight Kafka client for long-running producers/consumers. The code was based on James Roland's KafkaNet implementation, trimmed down and optimized significantly.

* Build Status: [![Build status](https://ci.appveyor.com/api/projects/status/5yra8edemc5kji46?svg=true)](https://ci.appveyor.com/project/Kelindar/misakai-kafka)
* NuGet Package: [Misakai.Kafka](https://www.nuget.org/packages/Misakai.Kafka/)

Examples
-----------
##### Producer
```csharp
var options = new KafkaOptions(new Uri("http://kafka1:9092"), new Uri("http://kafka2:9092"))
{
    Log = new ConsoleLog()
};
var router = new BrokerRouter(options);
var client = new Producer(router);

Task.Run(() =>
{
    var consumer = new Consumer(new ConsumerOptions("latencies", router));
    foreach (var data in consumer.Consume())
    {
       // ... process each message
    }
});

while (true)
{
    client.SendMessageAsync("latencies", new[] {
        "message"
    });
    Thread.Sleep(1);
}

client.Dispose();
router.Dispose();
```
