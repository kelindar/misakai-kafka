using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Misakai.Kafka
{
    /// <summary>
    /// This class is used for reading Kafka binary messages.
    /// </summary>
    public class BinaryReader
    {
        private readonly byte[] Buffer;
        private readonly int Size;
        private readonly int Offset;
        private int Index;

        /// <summary>
        /// Constructs a reader over a buffer.
        /// </summary>
        /// <param name="buffer">The buffer to construct the reader for.</param>
        public BinaryReader(byte[] buffer)
        {
            this.Buffer = buffer;
            this.Offset = 0;
            this.Size = buffer.Length;
            this.Index = 0;
        }

        /// <summary>
        /// Constructs a reader over a buffer.
        /// </summary>
        /// <param name="buffer">The buffer to construct the reader for.</param>
        public BinaryReader(byte[] buffer, int offset, int size)
        {
            this.Buffer = buffer;
            this.Offset = offset;
            this.Size = size;
            this.Index = offset;
        }

        /// <summary>
        /// Gets the current position in the array.
        /// </summary>
        public int Position
        {
            get { return Index; }
            set { Index = value; }
        }

        /// <summary>
        /// Gets whether the reader has any data or not.
        /// </summary>
        public bool HasData 
        {
            get { return this.Index < (this.Offset + this.Size); } 
        }

        /// <summary>
        /// Checks if we have enough data available to continue processing.
        /// </summary>
        /// <param name="requiredSize">The size we need to check.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Available(int requiredSize)
        {
            return (this.Offset + this.Size - this.Index) >= requiredSize;
        }

        /// <summary>
        /// Reads a byte from the buffer.
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte()
        {
            return this.Buffer[this.Index++];
        }

        /// <summary>
        /// Reads an int16 from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadInt16()
        {
            var val = (short)((this.Buffer[this.Index++] << 8) | this.Buffer[this.Index++]);
            return val;
        }

        /// <summary>
        /// Reads an int32 from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt32()
        {
            var val = (this.Buffer[this.Index++] << 24)
                    | (this.Buffer[this.Index++] << 16)
                    | (this.Buffer[this.Index++] << 8)
                    | this.Buffer[this.Index++];
            return val;
        }

        /// <summary>
        /// Reads an int64 from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadInt64()
        {
            var val = ((long)this.Buffer[this.Index++] << 56)
                    | ((long)this.Buffer[this.Index++] << 48)
                    | ((long)this.Buffer[this.Index++] << 40)
                    | ((long)this.Buffer[this.Index++] << 32)
                    | ((long)this.Buffer[this.Index++] << 24)
                    | ((long)this.Buffer[this.Index++] << 16)
                    | ((long)this.Buffer[this.Index++] << 8)
                    | ((long)this.Buffer[this.Index++]);
            return val;
        }

        /// <summary>
        /// Reads a string from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadInt16String()
        {
            var size = this.ReadInt16();
            if (size == -1) 
                return null;

            // Read a UTF-8 encoded string
            var val = Encoding.UTF8.GetString(
                this.Buffer, this.Index, size
            );
            this.Index += size;
            return val;
        }

        /// <summary>
        /// Reads a string from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadInt32String()
        {
            var size = this.ReadInt32();
            if (size == -1) 
                return null;

            // Read a UTF-8 encoded string
            var val = Encoding.UTF8.GetString(
                this.Buffer, this.Index, size
            );
            this.Index += size;
            return val;
        }

        /// <summary>
        /// Reads an array of bytes from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] ReadInt16Array()
        {
            var size = this.ReadInt16();
            if (size == -1)
                return null;
            if (size <= 0)
                return new byte[0];

            // Read into a separate buffer
            var buffer = new byte[size];
            BinaryHelper.Copy(this.Buffer, this.Index, buffer, 0, size);
            this.Index += size;
            return buffer;
        }

        /// <summary>
        /// Reads an array of bytes from the buffer. 
        /// </summary>
        /// <returns>The deserialized value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] ReadInt32Array()
        {
            var size = this.ReadInt32();
            if (size == -1) 
                return null;
            if (size <= 0)
                return new byte[0];

            // Read into a separate buffer
            var buffer = new byte[size];
            BinaryHelper.Copy(this.Buffer, this.Index, buffer, 0, size);
            this.Index += size;
            return buffer;
        }

    }
}
