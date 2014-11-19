using System;
using System.IO;
using Misakai.Kafka;

namespace Misakai.Kafka
{
    public abstract class KafkaRequest
    {
        /// <summary>
        /// From Documentation: 
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// 
        /// Kafka Protocol implementation:
        /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
        /// </summary>
        /// 

        protected const int ReplicaId = -1;
        protected const Int16 ApiVersion = 0;
        private string ClientTag = "MK";
        private int CorrelationId = 1;

        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string Client
        {
            get { return ClientTag; } 
            set { ClientTag = value; } 
        }

        /// <summary>
        /// Value supplied will be passed back in the response by the server unmodified. 
        /// It is useful for matching request and response between the client and server. 
        /// </summary>
        public int Correlation 
        {
            get { return CorrelationId; } 
            set { CorrelationId = value; } 
        }

    }
}