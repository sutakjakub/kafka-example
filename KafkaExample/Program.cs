using Confluent.Kafka;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaExample
{
    class Program
    {
        static void Main(string[] args)
        {
            //DoConsumeJobAsync().Wait();
            DoProduceJobAsync().Wait();
        }

        private static bool cancelled;

        private static async Task DoConsumeJobAsync()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "Foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var timer = new Timer(TimerTick, null, 15000, 1);
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                try
                {
                    consumer.Subscribe(new[] { "testdataother" });

                    while (!cancelled)
                    {
                        var consumeResult = consumer.Consume();
                        Console.WriteLine($"Cosunmer: {consumeResult.Message.Value}");
                    }
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        private static void TimerTick(object state)
        {
            cancelled = true;
        }

        private static async Task DoProduceJobAsync()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var task = producer.ProduceAsync("testdataother", new Message<Null, string>() { Value = "a log message" });
                await task.ContinueWith((t) =>
                {
                    if (t.IsFaulted)
                    {
                        Console.WriteLine($"Error occured");
                    }
                    else
                    {
                        Console.WriteLine($"Wrote offset: {t.Result.Offset}");
                    }
                });
            }
        }
    }
}
