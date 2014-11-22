using System;
using System.Threading.Tasks;
using Misakai.Kafka;
using System.Collections.Generic;
using System.Threading;
using System.Text;
using System.Linq;
using System.Diagnostics;

namespace Test
{
    class Program
    {

        static void Main(string[] args)
        {

            var options = new KafkaOptions(new Uri("http://kafka1:9092"), new Uri("http://kafka2:9092"))
            {
                Log = new ConsoleLog()
            };
            var router = new BrokerRouter(options);
            var client = new Producer(router);
            var timing = new RollingQueue<double>(50);
            var rate = new RollingQueue<double>(50);
            var second = DateTime.Now.Second;
            var count = 0;
            

            Task.Run(() =>
            {
                var consumer = new Consumer(new ConsumerOptions("latencies", router));
                var position = consumer.GetTopicOffsetAsync("latencies");
                position.Wait();
                consumer.SetOffsetPosition(
                    position.Result
                        .Select(p => new OffsetPosition(p.PartitionId, p.Offsets.First()))
                        .ToArray()
                    );

                foreach (var data in consumer.Consume())
                {
                    count++;
                    var rtt = (DateTime.Now - new DateTime(
                        long.Parse(Encoding.UTF8.GetString(data.Value))
                        )).TotalMilliseconds;

                    if (rtt < 1000)
                        timing.Enqueue(rtt);

                    if (second != DateTime.Now.Second)
                    {
                        second = DateTime.Now.Second;
                        rate.Enqueue(count);
                        count = 0;
                        Console.WriteLine("Rate: {0} pps.\t{1} ", 
                            rate.Average().ToString("N2") , (rtt < 1000) 
                            ? "RTT: " + timing.Average().ToString("N2") + " ms."
                            : string.Empty
                        );
                    }
                    
                }
            });



            while (true)
            {
                client.SendMessageAsync("latencies", new[] { new Message(DateTime.Now.Ticks.ToString()) });
                Thread.Sleep(1);
            }

            client.Dispose();
            router.Dispose();
        }
    }
}
