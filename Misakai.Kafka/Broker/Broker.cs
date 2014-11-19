using System;
using Misakai.Kafka;

namespace Misakai.Kafka
{
    public class Broker
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public Uri Address { get { return new Uri(string.Format("http://{0}:{1}", Host, Port));} }

        public static Broker FromStream(BinaryReader reader)
        {
            return new Broker
                {
                    BrokerId = reader.ReadInt32(),
                    Host = reader.ReadInt16String(),
                    Port = reader.ReadInt32()
                };
        }
    }
}
