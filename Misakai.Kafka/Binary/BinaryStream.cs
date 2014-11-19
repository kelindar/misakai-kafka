using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Misakai.Kafka
{
    /// <summary>
    /// This class is used for reading Kafka binary messages.
    /// </summary>
    public class BinaryStream
    {
        #region Constructors & Main Methods
        /// <summary>
        /// The underlying stream to write into.
        /// </summary>
        private readonly MemoryStream Stream = new MemoryStream();

        /// <summary>
        /// Internal format buffer.
        /// </summary>
        private readonly byte[] Buffer = new byte[32];

        /// <summary>
        /// Gets or sets the position within the stream.
        /// </summary>
        public long Position
        {
            get { return this.Stream.Position; }
            set { this.Stream.Position = value; }
        }

        /// <summary>
        /// Gets current length of the stream.
        /// </summary>
        public int Length
        {
            get { return (int)this.Stream.Length; }
        }

        /// <summary>
        /// Gets the underlying buffer of the binary stream.
        /// </summary>
        public byte[] GetBuffer()
        {
            return this.Stream.GetBuffer();
        }

        /// <summary>
        /// Asynchronously flushes the underlying socket.
        /// </summary>
        /// <returns></returns>
        public async Task<int> Flush()
        {
            // Flush asynchronously the underlying stream
            await this.Stream.FlushAsync();

            // Once flushed, return the length of the buffer we have
            return (int)this.Stream.Length;
        }
        #endregion

        /// <summary>
        /// Writes a sequence of bytes to the underlying stream
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteArray(byte[] buffer)
        {
            this.Stream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a sequence of bytes to the underlying stream
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteArray(byte[] buffer, int offset, int size)
        {
            this.Stream.Write(buffer, offset, size);
        }

        /// <summary>
        /// Writes a 1-byte unsigned integer value to the underlying stream.
        /// </summary>
        public void Write(byte value)
        {
            this.Stream.WriteByte(value);
        }

        /// <summary>
        /// Writes a 2-byte signed integer value to the underlying stream.
        /// </summary>
        public void Write(short value)
        {
            this.Buffer[0] = (byte)(value >> 8);
            this.Buffer[1] = (byte)value;

            this.Stream.Write(this.Buffer, 0, 2);
        }

        /// <summary>
        /// Writes a 4-byte signed integer value to the underlying stream.
        /// </summary>
        public void Write(int value)
        {
            this.Buffer[0] = (byte)(value >> 24);
            this.Buffer[1] = (byte)(value >> 16);
            this.Buffer[2] = (byte)(value >> 8);
            this.Buffer[3] = (byte)value;

            this.Stream.Write(this.Buffer, 0, 4);
        }


        /// <summary>
        /// Writes a 8-byte signed integer value to the underlying stream.
        /// </summary>
        public void Write(long value)
        {
            this.Buffer[0] = (byte)(value >> 56);
            this.Buffer[1] = (byte)(value >> 48);
            this.Buffer[2] = (byte)(value >> 40);
            this.Buffer[3] = (byte)(value >> 32);
            this.Buffer[4] = (byte)(value >> 24);
            this.Buffer[5] = (byte)(value >> 16);
            this.Buffer[6] = (byte)(value >> 8);
            this.Buffer[7] = (byte)value;

            this.Stream.Write(this.Buffer, 0, 8);
        }



        /// <summary>
        /// Writes a key to the underlying stream.
        /// </summary>
        public void Write(ApiKeyRequestType apiKey)
        {
            this.Write((short)apiKey);
        }


        /// <summary>
        /// Writes a string into the underlying stream.
        /// </summary>
        public void Write(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                // TODO: I'm not sure if there should be Int16 or Int32 in this case...
                this.Write((short)-1);
                return;
            }

            // Write the length of the string & UTF8 is array of bytes, no endianness.
            this.Write((short)value.Length);
            this.WriteArray(Encoding.UTF8.GetBytes(value));
        }

        /// <summary>
        /// Writes an array into the underlying buffer.
        /// </summary>
        public void Write(byte[] value)
        {
            if (value == null)
            {
                this.Write(-1);
                return;
            }

            this.Write(value.Length);
            this.WriteArray(value);
        }

        /// <summary>
        /// Writes a 4-byte placeholder and returns current position
        /// </summary>
        public int PutPlaceholder()
        {
            // Remember current position
            var position = (int)this.Position;

            this.Buffer[0] = 0;
            this.Buffer[1] = 0;
            this.Buffer[2] = 0;
            this.Buffer[3] = 0;

            // Write the placeholder
            this.Stream.Write(this.Buffer, 0, 4);
            return position;
        }

        /// <summary>
        /// Writes a 4-byte placeholder and returns current position
        /// </summary>
        public void WriteLengthAt(int offset)
        {
            // Remember current position
            var position = (int)this.Position;

            // Calculate the length to write
            var value = position - offset - 4;

            // Set the position
            this.Position = offset;

            this.Buffer[0] = (byte)(value >> 24);
            this.Buffer[1] = (byte)(value >> 16);
            this.Buffer[2] = (byte)(value >> 8);
            this.Buffer[3] = (byte)value;

            // Write the value
            this.Stream.Write(this.Buffer, 0, 4);
            
            // Set the position back
            this.Position = position;
        }

        /// <summary>
        /// Writes a 4-byte placeholder and returns current position
        /// </summary>
        public void WriteCrcAt(int offset)
        {
            // Remember current position
            var position = (int)this.Position;

            // Calculate the length & CRC
            var count = position - offset - 4;
            var crc = BinaryCrc32.ComputeHash(this.GetBuffer(), offset + 4, count);

            // Set the position
            this.Position = offset;

            // Write the value
            this.Stream.Write(crc, 0, 4);

            // Set the position back
            this.Position = position;
        }
    }
}
