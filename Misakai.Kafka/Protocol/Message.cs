using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Misakai.Kafka;

namespace Misakai.Kafka
{
  
    /// <summary>
    /// Message represents the data from a single event occurance.
    /// </summary>
    public sealed class Message
    {
        private const int MinimumMessageSize = 12;

        /// <summary>
        /// The log offset of this message as stored by the Kafka server.
        /// </summary>
        public long Offset;

        /// <summary>
        /// The partition id this offset is from.
        /// </summary>
        public int PartitionId;

        /// <summary>
        /// This is a version id used to allow backwards compatible evolution of the message binary format.  Reserved for future use.  
        /// </summary>
        public byte MagicNumber;

        /// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// </summary>
        public byte Attribute;

        /// <summary>
        /// Key value used for routing message to partitions.
        /// </summary>
        public byte[] Key;

        /// <summary>
        /// The message body contents.  Can contain compress message set.
        /// </summary>
        public byte[] Value;

        /// <summary>
        /// Creates an empty message.
        /// </summary>
        public Message()
        {

        }

        /// <summary>
        /// Convenience constructor will encode both the key and message to byte streams.
        /// Most of the time a message will be string based.
        /// </summary>
        /// <param name="key">The key value for the message.  Can be null.</param>
        /// <param name="value">The main content data of this message.</param>
        public Message(string value, string key = null)
        {
            this.Key = key == null
                ? null
                : Encoding.UTF8.GetBytes(key);

            this.Value = value.ToBytes();
            this.Attribute = 0;
            this.MagicNumber = 0;
            this.Offset = 0;
            this.PartitionId = 0;
        }


        /// <summary>
        /// Encodes a collection of messages into one byte[].  Encoded in order of list.
        /// </summary>
        /// <param name="messages">The collection of messages to encode together.</param>
        /// <returns>Encoded byte[] representing the collection of messages.</returns>
        public static void EncodeMessages(BinaryStream writer, IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                // Write the mesage
                writer.Write((long)0);

                // Placeholder for the length of the message
                var placeholder = writer.PutPlaceholder();

                // Placeholder for CRC of the payload
                var crc = writer.PutPlaceholder();

                // Write the body of the message
                writer.Write(message.MagicNumber);
                writer.Write(message.Attribute);
                writer.Write(message.Key);
                writer.Write(message.Value);

                // Write the CRC 
                writer.WriteCrcAt(crc);
                
                // Write the length of the message now
                writer.WriteLengthAt(placeholder);
            }
        }


        /// <summary>
        /// Decode a byte[] that represents a collection of messages.
        /// </summary>
        /// <param name="incoming">The byte[] encode as a message set from kafka.</param>
        /// <param name="partition">The partition to decode the message for.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[]</returns>
        public static IEnumerable<Message> DecodeMessages(byte[] incoming, int readOffset, int readSize, int partition)
        {
            var reader = new BinaryReader(incoming, readOffset, readSize);
            while (reader.HasData)
            {
                // If the message set hits against our max bytes wall on the fetch we will have a 
                // 1/2 completed message downloaded. The decode should guard against this situation
                if (reader.Available(MinimumMessageSize) == false)
                    yield break;

                // We read the offset of the message and the size and wait until we actually have
                // enough data to process the whole message.
                var offset = reader.ReadInt64();
                var messageSize = reader.ReadInt32();
                if (reader.Available(messageSize) == false)
                    yield break;

                // We now compute the hash and compare it with the checksum we received within
                // the message itself.
                var crc = BinaryCrc32.ComputeHash(incoming, reader.Position + 4, messageSize - 4);
                if (   crc[0] != reader.ReadByte()
                    || crc[1] != reader.ReadByte()
                    || crc[2] != reader.ReadByte()
                    || crc[3] != reader.ReadByte())
                    throw new FailCrcCheckException("CRC validation mismatch during message decoding.");

                var message = new Message
                {
                    Offset = offset,
                    MagicNumber = reader.ReadByte(),
                    Attribute = reader.ReadByte(),
                    Key = reader.ReadInt32Array(),
                    PartitionId = partition
                };

                // We don't support any codecs yet
                var codec = (MessageCodec)(ProtocolConstants.AttributeCodeMask & message.Attribute);
                if(codec != MessageCodec.CodecNone)
                    throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", codec));

                // Value is int32 prefixed byte-array
                message.Value = reader.ReadInt32Array();
                yield return message;
            }
        }

    }

    /// <summary>
    /// Payload represents a collection of messages to be posted to a specified Topic on specified Partition.
    /// </summary>
    public struct Payload
    {
        /// <summary>
        /// The topic that the payload belongs to.
        /// </summary>
        public string Topic;

        /// <summary>
        /// The partition of the payload.
        /// </summary>
        public int Partition;

        /// <summary>
        /// The encoding of the payload.
        /// </summary>
        public MessageCodec Codec;

        /// <summary>
        /// The payload itself.
        /// </summary>
        public List<Message> Messages;
    }



}
