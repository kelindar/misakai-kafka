using System;
using System.Threading.Tasks;
using Misakai.Kafka;
using System.Collections.Generic;
using System.Threading;
using System.Text;
using System.Linq;

namespace Test
{
    class Program
    {

        static void Main(string[] args)
        {

            var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"))
                {
                    Log = new ConsoleLog()
                };
            var router = new BrokerRouter(options);
            var client = new Producer(router);
            var queue = new RollingQueue<double>(50);
            var second = DateTime.Now.Second;
            
            Task.Run(() =>
                {
                    var consumer = new Consumer(new ConsumerOptions("latencies", router));
                    foreach (var data in consumer.Consume())
                    {

                        var rtt = (DateTime.Now - new DateTime(
                            long.Parse(Encoding.UTF8.GetString(data.Value))
                            )).TotalMilliseconds;
                        if (rtt < 1000)
                        {
                            queue.Enqueue(rtt);
                            if (second != DateTime.Now.Second)
                            {
                                second = DateTime.Now.Second;
                                Console.WriteLine(queue.Average() + " ms.");
                            }
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
