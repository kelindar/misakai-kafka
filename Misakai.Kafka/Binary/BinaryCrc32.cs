using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace Misakai.Kafka
{
    /// <summary>
    /// Single-threaded binary CRC32, optimized version. Original implementation by 
    /// Eugene Larchenko (http://dev.khsu.ru/el/crc32/).
    /// </summary>
    internal static class BinaryCrc32
    {
        private const uint kCrcPoly = 0xEDB88320;
        private const uint kInitial = 0xFFFFFFFF;
        private static readonly uint[] Table;
        private const uint CRC_NUM_TABLES = 8;

        static BinaryCrc32()
        {
            unchecked
            {
                Table = new uint[256 * CRC_NUM_TABLES];
                uint i;
                for (i = 0; i < 256; i++)
                {
                    uint r = i;
                    for (int j = 0; j < 8; j++)
                        r = (r >> 1) ^ (kCrcPoly & ~((r & 1) - 1));
                    Table[i] = r;
                }
                for (; i < 256 * CRC_NUM_TABLES; i++)
                {
                    uint r = Table[i - 256];
                    Table[i] = Table[r & 0xFF] ^ (r >> 8);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] ComputeHash(byte[] data)
        {
            return ComputeHash(data, 0, data.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] ComputeHash(byte[] data, int offset, int count)
        {
            if (count == 0)
                return null;

            var crc = ~Compute(data, offset, count);
            return new byte[]{
                (byte)(crc >> 24),
                (byte)(crc >> 16),
                (byte)(crc >> 8),
                (byte)(crc)
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Compute(byte[] data)
        {
            return Compute(data, 0, data.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Compute(byte[] data, int offset, int count)
        {
            if (count == 0)
                return 0;

            // important for performance!
            var table = BinaryCrc32.Table;
            uint crc = kInitial;

            for (; (offset & 7) != 0 && count != 0; count--)
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];

            if (count >= 8)
            {
                /*
                 * Idea from 7-zip project sources (http://7-zip.org/sdk.html)
                 */

                int to = (count - 8) & ~7;
                count -= to;
                to += offset;

                while (offset != to)
                {
                    crc ^= (uint)(data[offset] + (data[offset + 1] << 8) + (data[offset + 2] << 16) + (data[offset + 3] << 24));
                    uint high = (uint)(data[offset + 4] + (data[offset + 5] << 8) + (data[offset + 6] << 16) + (data[offset + 7] << 24));
                    offset += 8;

                    crc = table[(byte)crc + 0x700]
                        ^ table[(byte)(crc >>= 8) + 0x600]
                        ^ table[(byte)(crc >>= 8) + 0x500]
                        ^ table[/*(byte)*/(crc >> 8) + 0x400]
                        ^ table[(byte)(high) + 0x300]
                        ^ table[(byte)(high >>= 8) + 0x200]
                        ^ table[(byte)(high >>= 8) + 0x100]
                        ^ table[/*(byte)*/(high >> 8) + 0x000];
                }
            }

            while (count-- != 0)
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];

            return crc;
        }

    }
}