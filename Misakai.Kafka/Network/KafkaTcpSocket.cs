using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;

namespace Misakai.Kafka
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public event ReconnectionAttempDelegate OnReconnectionAttempt;
        public delegate void ReconnectionAttempDelegate(int attempt);

        private const int DefaultReconnectionTimeout = 500;
        private const int DefaultReconnectionTimeoutMultiplier = 2;

        private readonly SemaphoreSlim _singleReaderSemaphore = new SemaphoreSlim(1, 1);
        private readonly ThreadWall _getConnectedClientThreadWall = new ThreadWall(ThreadWallState.Blocked);
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly IKafkaLog _log;
        private readonly KafkaEndpoint _endpoint;

        private TcpClient _client;
        private int _ensureOneThread;
        private int _disposeCount;
        private Task _clientConnectingTask = null;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="delayConnectAttemptMS">Time in milliseconds to delay the initial connection attempt to the given server.</param>
        public KafkaTcpSocket(IKafkaLog log, KafkaEndpoint endpoint, int delayConnectAttemptMS = 0)
        {
            _log = log;
            _endpoint = endpoint;
            Task.Delay(TimeSpan.FromMilliseconds(delayConnectAttemptMS)).ContinueWith(x => TriggerReconnection());
        }

        #region Interface Implementation...
        /// <summary>
        /// The IP Endpoint to the server.
        /// </summary>
        public KafkaEndpoint Endpoint { get { return _endpoint; } }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize)
        {
            return EnsureReadAsync(readSize, _disposeToken.Token);
        }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            return EnsureReadAsync(readSize, cancellationToken);
        }


        /// <summary>
        /// Convenience function to write full buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(BinaryStream buffer)
        {
            return EnsureWriteAsync(buffer, _disposeToken.Token);
        }

       
        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(BinaryStream buffer, CancellationToken cancellationToken)
        {
            return EnsureWriteAsync(buffer, cancellationToken);
        }
        #endregion

        private Task<TcpClient> GetClientAsync()
        {
            return _getConnectedClientThreadWall.RequestPassageAsync().ContinueWith(t =>
                {
                    if (_client == null)
                    {
                        throw new ServerUnreachableException(string.Format("Connection to {0} was not established.", _endpoint));
                    }
                    return _client;
                });
        }

        private async Task EnsureWriteAsync(BinaryStream buffer, CancellationToken cancellationToken)
        {
            try
            {
                // Get the client first
                var client = await GetClientAsync();

                // We now need to flush asynchronously the stream
                var length = await buffer.Flush();

                // Now we can send the buffer.
                await client.GetStream()
                    .WriteAsync(buffer.GetBuffer(), 0, length, cancellationToken);
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Object is disposing.");
                throw;
            }
        }

        private async Task<byte[]> EnsureReadAsync(int readSize, CancellationToken token)
        {
            var cancelTaskToken = new CancellationTokenRegistration();
            try
            {
                await _singleReaderSemaphore.WaitAsync(token);

                var buffer = new byte[readSize];
                var length = 0;
                var offset = 0;

                while (length < readSize)
                {
                    readSize = readSize - length;
                    var client = await GetClientAsync();

                    // Read asynchronously to the receive buffer
                    length = await client.GetStream()
                        .ReadAsync(buffer, offset, readSize, token)
                        .WithCancellation(token);
                    if (length <= 0)
                    {
                        // Attempt to trigger a reconnection and throw an error
                        this.TriggerReconnection();
                        throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _endpoint));
                    }

                    // Increase the offset in the buffer
                    offset += length;
                }

                return buffer;
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested)
                    throw new ObjectDisposedException("Object is disposing.");
                //TODO add exception test here to see if an exception made us lose a connection Issue #17
                throw;
            }
            finally
            {
                cancelTaskToken.Dispose();
                _singleReaderSemaphore.Release();
            }
        }

        private void TriggerReconnection()
        {
            //only allow one thread to trigger a reconnection, all other requests should be ignored.
            if (Interlocked.Increment(ref _ensureOneThread) == 1)
            {
                //block downstream from getting a socket client until reconnected
                _getConnectedClientThreadWall.Block();
                _clientConnectingTask = ReEstablishConnectionAsync()
                    .ContinueWith(x =>
                    {
                        Interlocked.Decrement(ref _ensureOneThread);
                        _getConnectedClientThreadWall.Release();
                    });
            }
            else
            {
                Interlocked.Decrement(ref _ensureOneThread);
            }
        }

        private async Task<TcpClient> ReEstablishConnectionAsync()
        {
            var attempts = 1;
            var reconnectionDelay = DefaultReconnectionTimeout;
            _log.WarnFormat("No connection to:{0}.  Attempting to re-connect...", _endpoint);

            //clean up existing client
            using (_client) { _client = null; }

            while (_disposeToken.IsCancellationRequested == false)
            {
                try
                {
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts++);
                    _client = new TcpClient();
                    await _client.ConnectAsync(_endpoint.Endpoint.Address, _endpoint.Endpoint.Port);
                    _log.WarnFormat("Connection established to:{0}.", _endpoint);
                    return _client;
                }
                catch
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    _log.WarnFormat("Failed re-connection to:{0}.  Will retry in:{1}", _endpoint, reconnectionDelay);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay), _disposeToken.Token);
            }

            return _client;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) 
                return;

            if (_disposeToken != null)
                _disposeToken.Cancel();
            
            using (_disposeToken)
            using (_client)
            {
                if (_clientConnectingTask != null)
                    _clientConnectingTask.Wait(TimeSpan.FromSeconds(5));
            }
        }
    }
}
