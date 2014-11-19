using System;
using System.Collections.Generic;
using System.Linq;
using Misakai.Kafka;

namespace Misakai.Kafka
{
	/// <summary>
	/// A funky Protocol for requesting the starting offset of each segment for the requested partition 
	/// </summary>
    public sealed class OffsetRequest : KafkaRequest, IKafkaRequest<OffsetResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Offset; } }
        public List<Offset> Offsets { get; set; }

        public void Encode(BinaryStream writer)
        {
            if (this.Offsets == null)
                this.Offsets = new List<Offset>();

            var topicGroups = this.Offsets
                .GroupBy(x => x.Topic).ToList();

            // Here we put a placeholder for the length
            var placeholder = writer.PutPlaceholder();

            // Encode the header first
            EncodeHeader(writer, this);

            // Encode the metadata now
            writer.Write(ReplicaId);
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
                        // Write offset info
                        writer.Write(partition.Key);
                        writer.Write(offset.Time);
                        writer.Write(offset.MaxOffsets);
                    }
                }
            }


            // Write the length at the placeholder
            writer.WriteLengthAt(placeholder);
        }

        public IEnumerable<OffsetResponse> Decode(byte[] data)
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
                    var response = new OffsetResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt32(),
                        Error = stream.ReadInt16(),
                        Offsets = new List<long>()
                    };
                    var offsetCount = stream.ReadInt32();
                    for (int k = 0; k < offsetCount; k++)
                    {
                        response.Offsets.Add(stream.ReadInt64());
                    }

                    yield return response;
                }
            }
        }

    }

    public class Offset
    {
        public Offset()
        {
            Time = -1;
            MaxOffsets = 1;
        }
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset. 
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public long Time { get; set; }
        public int MaxOffsets { get; set; }
    }

    public class OffsetResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
        public List<long> Offsets { get; set; }
    }

    public class OffsetPosition
    {
        public OffsetPosition() { }
        public OffsetPosition(int partitionId, long offset)
        {
            PartitionId = partitionId;
            Offset = offset;
        }
        public int PartitionId { get; set; }
        public long Offset { get; set; }

        public override string ToString()
        {
            return string.Format("PartitionId:{0}, Offset:{1}", PartitionId, Offset);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((OffsetPosition)obj);
        }

        protected bool Equals(OffsetPosition other)
        {
            return PartitionId == other.PartitionId && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (PartitionId * 397) ^ Offset.GetHashCode();
            }
        }
    }
}