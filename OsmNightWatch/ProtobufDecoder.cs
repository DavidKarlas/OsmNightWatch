using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static OsmNightWatch.ProtobufHelpers;

namespace OsmNightWatch
{
    internal class ProtobufDecoder
    {
        /// <summary>
        /// Since the int64 format is inefficient for negative numbers we have avoided to implement it.
        /// The same functionality can be achieved using: (long)ReadUInt64(stream);
        /// </summary>
        [Obsolete("Use (long)ReadUInt64(stream); instead")]
        public static int ReadInt64(Stream stream)
        {
            return (int)ReadUInt64(stream);
        }

        /// <summary>
        /// Since the int32 format is inefficient for negative numbers we have avoided to implement it.
        /// The same functionality can be achieved using: (int)ReadUInt64(stream);
        /// </summary>
        [Obsolete("Use (int)ReadUInt64(stream); //yes 64")]
        public static int ReadInt32(Stream stream)
        {
            return (int)ReadUInt64(stream);
        }

        /// <summary>
        /// Zig-zag signed VarInt format
        /// </summary>
        public static int ReadZInt32(Stream stream)
        {
            uint val = ReadUInt32(stream);
            return (int)(val >> 1) ^ ((int)(val << 31) >> 31);
        }

        /// <summary>
        /// Unsigned VarInt format
        /// Do not use to read int32, use ReadUint64 for that.
        /// </summary>
        public static uint ReadUInt32(Stream stream)
        {
            uint val = 0;

            for (int n = 0; n < 5; n++)
            {
                int b = stream.ReadByte();
                if (b < 0)
                    throw new IOException("Stream ended too early");

                //Check that it fits in 32 bits
                if ((n == 4) && (b & 0xF0) != 0)
                    throw new Exception("Got larger VarInt than 32bit unsigned");
                //End of check

                if ((b & 0x80) == 0)
                    return val | (uint)b << (7 * n);

                val |= (uint)(b & 0x7F) << (7 * n);
            }

            throw new Exception("Got larger VarInt than 32bit unsigned");
        }


        /// <summary>
        /// Zig-zag signed VarInt format
        /// </summary>
        public static long ReadZInt64(Stream stream)
        {
            ulong val = ReadUInt64(stream);
            return (long)(val >> 1) ^ ((long)(val << 63) >> 63);
        }

        /// <summary>
        /// Unsigned VarInt format
        /// </summary>
        public static ulong ReadUInt64(Stream stream)
        {
            ulong val = 0;

            for (int n = 0; n < 10; n++)
            {
                int b = stream.ReadByte();
                if (b < 0)
                    throw new IOException("Stream ended too early");

                //Check that it fits in 64 bits
                if ((n == 9) && (b & 0xFE) != 0)
                    throw new Exception("Got larger VarInt than 64 bit unsigned");
                //End of check

                if ((b & 0x80) == 0)
                    return val | (ulong)b << (7 * n);

                val |= (ulong)(b & 0x7F) << (7 * n);
            }

            throw new Exception("Got larger VarInt than 64 bit unsigned");
        }


        internal static int TryParseUInt32Varint(int offset, bool trimNegative, out uint value, ReadOnlySpan<byte> span)
        {
            if ((uint)offset >= (uint)span.Length)
            {
                value = 0;
                return 0;
            }

            value = span[offset++];
            if ((value & 0x80) == 0) return 1;
            value &= 0x7F;

            if ((uint)offset >= (uint)span.Length) ThrowEoF();
            uint chunk = span[offset++];
            value |= (chunk & 0x7F) << 7;
            if ((chunk & 0x80) == 0) return 2;

            if ((uint)offset >= (uint)span.Length) ThrowEoF();
            chunk = span[offset++];
            value |= (chunk & 0x7F) << 14;
            if ((chunk & 0x80) == 0) return 3;

            if ((uint)offset >= (uint)span.Length) ThrowEoF();
            chunk = span[offset++];
            value |= (chunk & 0x7F) << 21;
            if ((chunk & 0x80) == 0) return 4;

            if ((uint)offset >= (uint)span.Length) ThrowEoF();
            chunk = span[offset++];
            value |= chunk << 28; // can only use 4 bits from this chunk
            if ((chunk & 0xF0) == 0) return 5;

            if (trimNegative // allow for -ve values
                && (chunk & 0xF0) == 0xF0
                && offset + 4 < (uint)span.Length
                    && span[offset] == 0xFF
                    && span[offset + 1] == 0xFF
                    && span[offset + 2] == 0xFF
                    && span[offset + 3] == 0xFF
                    && span[offset + 4] == 0x01)
            {
                return 10;
            }

            ThrowOverflow();
            return 0;
        }
    }
}
