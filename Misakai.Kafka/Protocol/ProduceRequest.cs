using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Misakai.Kafka;

namespace Misakai.Kafka
{
    public sealed class ProduceRequest : KafkaRequest, IKafkaRequest<ProduceResponse>
    {
        private Func<MessageCodec, byte[], byte[]> compressionFunction;

        public ProduceRequest(Func<MessageCodec, byte[], byte[]> compression = null)
        {
            compressionFunction = compression;    
        }

        /// <summary>
        /// Indicates the type of kafka encoding this request is.
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Produce; } }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public int TimeoutMS = 1000;

        /// <summary>
        /// Level of ack required by kafka.  0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public Int16 Acks = 1;

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public List<Payload> Payload = new List<Payload>();
       

        public void Encode(BinaryStream writer)
        {
            if (this.Payload == null)
                this.Payload = new List<Payload>();

            var groupedPayloads = (from p in this.Payload
                                   group p by new
                                   {
                                       p.Topic,
                                       p.Partition,
                                       p.Codec
                                   } into tpc
                                   select tpc).ToList();

            // Here we put a placeholder for the length
            var placeholder = writer.PutPlaceholder();

            // Encode the header first
            EncodeHeader(writer, this);

            // Encode the metadata now
            writer.Write(this.Acks);
            writer.Write(this.TimeoutMS);
            writer.Write(groupedPayloads.Count);


            foreach (var groupedPayload in groupedPayloads)
            {
                var payloads = groupedPayload.ToList();

                // We do not support any compression right now
                if (groupedPayload.Key.Codec != MessageCodec.CodecNone)
                    throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", groupedPayload.Key.Codec));

                // Write payload group
                writer.Write(groupedPayload.Key.Topic);
                writer.Write(payloads.Count);
                writer.Write(groupedPayload.Key.Partition);

                // Placeholder for the length
                var length = writer.PutPlaceholder();

                Message.EncodeMessages(writer,
                    payloads.SelectMany(x => x.Messages)
                    );

                // Write the total length
                writer.WriteLengthAt(length);
            }

            // Write the length at the placeholder
            writer.WriteLengthAt(placeholder);
        }



        public IEnumerable<ProduceResponse> Decode(byte[] data)
        {
            var stream = new BinaryReader(data);

            var correlationId = stream.ReadInt32();

            var topicCount = stream.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new ProduceResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt32(),
                        Error = stream.ReadInt16(),
                        Offset = stream.ReadInt64()
                    };

                    yield return response;
                }
            }
        }

    }

    public class ProduceResponse
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public Int16 Error { get; set; }
        public long Offset { get; set; }
    }
}