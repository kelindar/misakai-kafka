using System;
using System.Collections.Generic;
using System.Linq;

using Misakai.Kafka;

namespace Misakai.Kafka
{
    /// <summary>
    /// Class that represents both the request and the response from a kafka server of requesting a stored offset value
    /// for a given consumer group.  Essentially this part of the api allows a user to save/load a given offset position
    /// under any abritrary name.
    /// </summary>
    public class OffsetFetchRequest : KafkaRequest, IKafkaRequest<OffsetFetchResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetFetch; } }
        public string ConsumerGroup { get; set; }
        public List<OffsetFetch> Topics { get; set; }

        public void Encode(BinaryStream writer)
        {
            EncodeOffsetFetchRequest(writer, this);
        }

        protected void EncodeOffsetFetchRequest(BinaryStream writer, OffsetFetchRequest request)
        {
            if (request.Topics == null)
                request.Topics = new List<OffsetFetch>();

            var topicGroups = request.Topics
                .GroupBy(x => x.Topic).ToList();

            // Here we put a placeholder for the length
            var placeholder = writer.PutPlaceholder();

            // Encode the header first
            EncodeHeader(writer, request);

            // Write the consumer group
            writer.Write(ConsumerGroup);
            writer.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup
                    .GroupBy(x => x.PartitionId).ToList();

                writer.Write(topicGroup.Key);
                writer.Write(partitions.Count);

                foreach (var partition in partitions)
                {
                    foreach (var offset in partition)
                    {
                        writer.Write(offset.PartitionId);
                    }
                }
            }

            // Write the length at the placeholder
            writer.WriteLengthAt(placeholder);
        }

        public IEnumerable<OffsetFetchResponse> Decode(byte[] payload)
        {
            return DecodeOffsetFetchResponse(payload);
        }


        protected IEnumerable<OffsetFetchResponse> DecodeOffsetFetchResponse(byte[] data)
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
                    var response = new OffsetFetchResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt32(),
                        Offset = stream.ReadInt64(),
                        MetaData = stream.ReadInt16String(),
                        Error = stream.ReadInt16()
                    };
                    yield return response;
                }
            }
        }

    }

    public class OffsetFetch
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
    }

    public class OffsetFetchResponse
    {
        /// <summary>
        /// The name of the topic this response entry is for.
        /// </summary>
        public string Topic;
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public Int32 PartitionId;
        /// <summary>
        /// The offset position saved to the server.
        /// </summary>
        public Int64 Offset;
        /// <summary>
        /// Any arbitrary metadata stored during a CommitRequest.
        /// </summary>
        public string MetaData;
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public Int16 Error;

        public override string ToString()
        {
            return string.Format("[OffsetFetchResponse TopicName={0}, PartitionID={1}, Offset={2}, MetaData={3}, ErrorCode={4}]", Topic, PartitionId, Offset, MetaData, Error);
        }

    }
}
