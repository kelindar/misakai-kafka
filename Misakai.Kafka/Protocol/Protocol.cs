using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Misakai.Kafka
{
    public enum ApiKeyRequestType
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        LeaderAndIsr = 4,
        StopReplica = 5,
        OffsetCommit = 8,
        OffsetFetch = 9,
        ConsumerMetadataRequest = 10
    }

    public enum ErrorResponseCode
    {
        NoError = 0,
        Unknown = -1,
        OffsetOutOfRange = 1,
        InvalidMessage = 2,
        UnknownTopicOrPartition = 3,
        InvalidMessageSize = 4,
        LeaderNotAvailable = 5,
        NotLeaderForPartition = 6,
        RequestTimedOut = 7,
        BrokerNotAvailable = 8,
        ReplicaNotAvailable = 9,
        MessageSizeTooLarge = 10,
        StaleControllerEpochCode = 11,
        OffsetMetadataTooLargeCode = 12,
        OffsetsLoadInProgressCode = 14,
        ConsumerCoordinatorNotAvailableCode = 15,
        NotCoordinatorForConsumerCode = 16
    }

    public struct ProtocolConstants
    {
        public static byte AttributeCodeMask = 0x03;
    }

    public enum MessageCodec
    {
        CodecNone = 0x00,
        CodecGzip = 0x01,
        CodecSnappy = 0x02
    }

    public class FailCrcCheckException : Exception
    {
        public FailCrcCheckException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ResponseTimeoutException : Exception
    {
        public ResponseTimeoutException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class InvalidPartitionException : Exception
    {
        public InvalidPartitionException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ServerDisconnectedException : Exception
    {
        public ServerDisconnectedException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class ServerUnreachableException : Exception
    {
        public ServerUnreachableException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class InvalidTopicMetadataException : Exception
    {
        public InvalidTopicMetadataException(ErrorResponseCode code, string message, params object[] args) : base(string.Format(message, args))
        {
            ErrorResponseCode = code;
        }
        public ErrorResponseCode ErrorResponseCode { get; private set; }
    }

    public class LeaderNotFoundException : Exception
    {
        public LeaderNotFoundException(string message, params object[] args) : base(string.Format(message, args)) { }
    }

    public class UnresolvedHostnameException : Exception
    {
        public UnresolvedHostnameException(string message, params object[] args) : base(string.Format(message, args)) { }
    }
}
