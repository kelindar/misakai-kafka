using System;
using System.Collections.Generic;
using System.Linq;
using Misakai.Kafka;

namespace Misakai.Kafka
{
    /// <summary>
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the 
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public sealed class OffsetCommitRequest : KafkaRequest, IKafkaRequest<OffsetCommitResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetCommit; } }
        public string ConsumerGroup { get; set; }
        public List<OffsetCommit> OffsetCommits { get; set; }

        public void Encode(BinaryStream writer)
        {
            if (this.OffsetCommits == null)
                this.OffsetCommits = new List<OffsetCommit>();

            var topicGroups = this.OffsetCommits
                .GroupBy(x => x.Topic).ToList();

            // Here we put a placeholder for the length
            var placeholder = writer.PutPlaceholder();

            // Encode the header first
            EncodeHeader(writer, this);

            // Write topic groups
            writer.Write(this.ConsumerGroup);
            writer.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup
                    .GroupBy(x => x.PartitionId).ToList();

                // Write topic group and #partitions
                writer.Write(topicGroup.Key);
                writer.Write(partitions.Count);

                foreach (var partition in partitions)
                {
                    foreach (var commit in partition)
                    {
                        // Write partition info
                        writer.Write(partition.Key);
                        writer.Write(commit.Offset);
                        writer.Write(commit.TimeStamp);
                        writer.Write(commit.Metadata);
                    }
                }
            }


            // Write the length at the placeholder
            writer.WriteLengthAt(placeholder);
        }

        public IEnumerable<OffsetCommitResponse> Decode(byte[] data)
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
                    var response = new OffsetCommitResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt32(),
                        Error = stream.ReadInt16()
                    };

                    yield return response;
                }
            }
        }


    }

    public class OffsetCommit
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
        public long Offset { get; set; }

        /// <summary>
        /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
        /// </summary>
        public long TimeStamp { get; set; }

        /// <summary>
        /// Descriptive metadata about this commit.
        /// </summary>
        public string Metadata { get; set; }

        public OffsetCommit()
        {
            TimeStamp = -1;
        }
    
    }

    public class OffsetCommitResponse
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
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public Int16 Error;
    }
}