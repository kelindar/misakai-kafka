using System.Collections.Generic;
using System.Linq;

namespace Misakai.Kafka
{
    public class MetadataRequest : KafkaRequest, IKafkaRequest<MetadataResponse>
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.MetaData; } }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public List<string> Topics { get; set; }

        public void Encode(BinaryStream writer)
        {
            EncodeMetadataRequest(writer, this);
        }

        public IEnumerable<MetadataResponse> Decode(byte[] payload)
        {
            return new[] { DecodeMetadataResponse(payload) };
        }

        /// <summary>
        /// Encode a request for metadata about topic and broker information.
        /// </summary>
        /// <param name="request">The MetaDataRequest to encode.</param>
        /// <returns>Encoded byte[] representing the request.</returns>
        /// <remarks>Format: (MessageSize), Header, ix(hs)</remarks>
        private void EncodeMetadataRequest(BinaryStream writer, MetadataRequest request)
        {
            if (request.Topics == null)
                request.Topics = new List<string>();

            // Here we put a placeholder for the length
            var placeholder = writer.PutPlaceholder();

            // Encode the header first
            EncodeHeader(writer, request);

            // Write the number of topics 
            writer.Write(request.Topics.Count);

            // Write each topic
            foreach(var topic in request.Topics)
            {
                writer.Write(topic);
            }

            // Write the length at the placeholder
            writer.WriteLengthAt(placeholder);
        }

        /// <summary>
        /// Decode the metadata response from kafka server.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private MetadataResponse DecodeMetadataResponse(byte[] data)
        {
            var stream = new BinaryReader(data);
            var response = new MetadataResponse();
            response.CorrelationId = stream.ReadInt32();

            var brokerCount = stream.ReadInt32();
            for (var i = 0; i < brokerCount; i++)
            {
                response.Brokers.Add(Broker.FromStream(stream));
            }

            var topicCount = stream.ReadInt32();
            for (var i = 0; i < topicCount; i++)
            {
                response.Topics.Add(Topic.FromStream(stream));
            }

            return response;
        }

    }

    public class MetadataResponse
    {
        public int CorrelationId { get; set; }
        public MetadataResponse()
        {
            Brokers = new List<Broker>();
            Topics = new List<Topic>();
        }

        public List<Broker> Brokers { get; set; }
        public List<Topic> Topics { get; set; }
    }
}