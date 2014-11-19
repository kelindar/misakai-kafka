using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Misakai.Kafka
{
    internal static class BinaryHelper
    {
        /// <summary>
        /// Copies a chunk of memory.
        /// </summary>
        /// <param name="source">The source buffer to read.</param>
        /// <param name="sourceOffset">The source offset to read.</param>
        /// <param name="destination">The destination buffer to write.</param>
        /// <param name="destinationOffset">The destination offset to write.</param>
        /// <param name="length">The lenght of bytes to copy.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static void Copy(byte[] source, int sourceOffset, byte[] destination, int destinationOffset, int length)
        {
            // Write to the block
            if (length < 512)
            {
                // Copy memory with a fast method
                fixed (byte* ptr1 = destination)
                fixed (byte* ptr2 = source)
                {
                    // Go to the appropriate offsets
                    var dest = ptr1 + destinationOffset;
                    var src = ptr2 + sourceOffset;

                    if (length >= 16)
                    {
                        do
                        {

                            *(long*)dest = *(long*)src;
                            *(long*)(dest + 8) = *(long*)(src + 8);
                            dest += 16;
                            src += 16;
                        }
                        while ((length -= 16) >= 16);
                    }
                    if (length > 0)
                    {
                        if ((length & 8) != 0)
                        {
                            *(long*)dest = *(long*)src;
                            dest += 8;
                            src += 8;
                        }
                        if ((length & 4) != 0)
                        {
                            *(int*)dest = *(int*)src;
                            dest += 4;
                            src += 4;
                        }
                        if ((length & 2) != 0)
                        {
                            *(short*)dest = *(short*)src;
                            dest += 2;
                            src += 2;
                        }
                        if ((length & 1) != 0)
                        {
                            byte* finalByte = dest;
                            dest = finalByte + 1;
                            byte* finalByteSrc = src;
                            src = finalByteSrc + 1;
                            *finalByte = *finalByteSrc;
                        }
                    }
                }
            }
            else
            {
                // Copy memory with standard method
                Buffer.BlockCopy(source, sourceOffset, destination, destinationOffset, length);
            }

        }

        public static byte[] ToBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            //UTF8 is array of bytes, no endianness
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToBytes(this Int32 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }


        public static Int32 ToInt32(this byte[] value)
        {
            return BitConverter.ToInt32(value.Reverse().ToArray(), 0);
        }

        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task<T> WithCancellation<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();

            using (cancellationToken.Register(source => ((TaskCompletionSource<bool>)source).TrySetResult(true), tcs))
            {
                if (task != await Task.WhenAny(task, tcs.Task))
                {
                    throw new OperationCanceledException(cancellationToken);
                }
            }

            return await task;
        }
    }
}
