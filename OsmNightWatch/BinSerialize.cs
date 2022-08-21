using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public static unsafe class BinSerialize
    {
        private static Encoding uft8 = Encoding.UTF8;
        private static Decoder utf8decoder = Encoding.UTF8.GetDecoder();

        public static void ThrowForBigEndian()
        {
            if (!BitConverter.IsLittleEndian)
                throw new NotSupportedException("BigEndian systems are not supported at this time.");
        }

        /// <summary>
        /// Read a packed integer.
        /// </summary>
        /// <remarks>
        /// See <see cref="WritePackedInteger"/> for more information.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Unpacked integer.</returns>
        public static int ReadZigzagInteger(ref ReadOnlySpan<byte> span)
        {
            var zigzagged = ReadPackedUnsignedInteger(ref span);
            return FromZigZagEncoding(zigzagged);
        }

        /// <summary>
        /// Read a packed integer.
        /// </summary>
        /// <remarks>
        /// See <see cref="WritePackedInteger"/> for more information.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Unpacked integer.</returns>
        public static long ReadPackedLong(ref ReadOnlySpan<byte> span)
        {
            return (long)ReadPackedULong(ref span);
        }
        public static int ReadPackedInt(ref ReadOnlySpan<byte> span)
        {
            return (int)ReadPackedULong(ref span);
        }

        public static long ReadZigZagLong(ref ReadOnlySpan<byte> span)
        {
            ulong val = ReadPackedULong(ref span);
            return (long)(val >> 1) ^ ((long)(val << 63) >> 63);
        }

        public static ulong ReadPackedULong(ref ReadOnlySpan<byte> span)
        {
            ulong val = 0;

            for (int n = 0; n < 10; n++)
            {
                ulong b = ReadByte(ref span);
                if ((b & 0x80) == 0)
                    return val | (ulong)b << (7 * n);

                val |= (ulong)(b & 0x7F) << (7 * n);
            }

            throw new Exception("Got larger VarInt than 64 bit unsigned");
        }

        /// <summary>
        /// Read a packed unsigned integer.
        /// </summary>
        /// <remarks>
        /// See <see cref="WritePackedUnsignedInteger"/> for more information.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Unpacked unsigned integer.</returns>
        public static uint ReadPackedUnsignedInteger(ref ReadOnlySpan<byte> span)
        {
            /* Read 7 bits of integer data and then the 8th bit indicates wether more data will follow.
            More info: https://en.wikipedia.org/wiki/Variable-length_quantity */

            uint result = 0;
            var resultBitOffset = 0;
            while (true)
            {
                var data = ReadByte(ref span);

                // Mask of the first 7 bits of the data and then 'apply' it to the result.
                result |= (uint)(data & 0b0111_1111) << resultBitOffset;

                // Check the last bit to see if this was the end.
                if ((data & 0b1000_0000) == 0)
                    break;

                // Increment the offset so the next iteration points at the next bits of the result.
                resultBitOffset += 7;
            }

            return result;
        }

        /// <summary>
        /// Read a packed unsigned integer.
        /// </summary>
        /// <remarks>
        /// See <see cref="WritePackedUnsignedInteger"/> for more information.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Unpacked unsigned integer.</returns>
        public static ulong ReadPackedUnsignedLong(ref ReadOnlySpan<byte> span)
        {
            /* Read 7 bits of integer data and then the 8th bit indicates wether more data will follow.
            More info: https://en.wikipedia.org/wiki/Variable-length_quantity */

            ulong result = 0;
            var resultBitOffset = 0;
            while (true)
            {
                var data = ReadByte(ref span);

                // Mask of the first 7 bits of the data and then 'apply' it to the result.
                result |= (uint)(data & 0b0111_1111) << resultBitOffset;

                // Check the last bit to see if this was the end.
                if ((data & 0b1000_0000) == 0)
                    break;

                // Increment the offset so the next iteration points at the next bits of the result.
                resultBitOffset += 7;
            }

            return result;
        }

        /// <summary>
        /// Check how many many bytes it will take to write the given value as a packed integer.
        /// </summary>
        /// <remarks>
        /// See <see cref="WritePackedInteger"/> for more information (including a size-table).
        /// </remarks>
        /// <param name="value">Value to check.</param>
        /// <returns>Number of bytes it will take.</returns>
        public static int GetSizeForPackedInteger(int value)
        {
            var zigzagged = ToZigZagEncoding(value);
            return GetSizeForPackedUnsignedInteger(zigzagged);
        }

        /// <summary>
        /// Check how many many bytes it will take to write the given value as a packed unsigned integer.
        /// </summary>
        /// <remarks>
        /// See <see cref="WritePackedUnsignedInteger"/> for more information (including a size-table).
        /// </remarks>
        /// <param name="value">Value to check.</param>
        /// <returns>Number of bytes it will take.</returns>
        public static int GetSizeForPackedUnsignedInteger(uint value)
        {
            /* Check how many 7 bit values we need to store the integer, for more info see
            'WritePackedUnsignedInteger' implementation. */

            var bytes = 1;
            while (value > 0b0111_1111)
            {
                value >>= 7;
                bytes++;
            }

            return bytes;
        }

        /// <summary>
        /// Pack a integer and write it.
        /// Uses a variable-length encoding scheme.
        /// </summary>
        /// <remarks>
        /// Size table:
        /// less then -134217729 = 5 bytes
        /// -134217728 to -1048577 = 4 bytes
        /// -1048576 to -8193 = 3 bytes
        /// -8192 to -65 = 2 bytes
        /// -64 to 63 = 1 bytes
        /// 64 to 8191 = 2 bytes
        /// 8192 to 1048575 = 3 bytes
        /// 1048576 to 134217727 = 4 bytes
        /// more then 134217728 = 5 bytes
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="value">Value to pack and write.</param>
        public static void WritePackedInteger(ref Span<byte> span, int value)
        {
            var zigzagged = ToZigZagEncoding(value);
            WritePackedUnsignedInteger(ref span, zigzagged);
        }

        internal static void EnsureProtoIndexAndType(ref ReadOnlySpan<byte> span, int index, int type)
        {
            var value = ReadByte(ref span);
            if (type != (value & 0b111))
                throw new Exception();
            if (index != value >> 3)
                throw new Exception();
        }

        internal static (byte index, byte type) ReadProtoIndexAndType(ref ReadOnlySpan<byte> span)
        {
            var value = ReadByte(ref span);
            return ((byte)(value >> 3), (byte)(value & 0b111));
        }

        /// <summary>
        /// Pack a unsigned integer and write it.
        /// Uses a variable-length encoding scheme.
        /// </summary>
        /// <remarks>
        /// Size table:
        /// 0 to 127 = 1 bytes
        /// 128 to 16383 = 2 bytes
        /// 16384 to 2097151 = 3 bytes
        /// 2097152 to 268435455 = 4 bytes
        /// more then 268435456 = 5 bytes
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="value">Value to pack and write.</param>
        public static void WritePackedUnsignedInteger(ref Span<byte> span, uint value)
        {
            /* Write 7 bits of integer data and then the 8th bit indicates wether more data will follow.
            More info: https://en.wikipedia.org/wiki/Variable-length_quantity */

            // As long as we have more data left then we can fit into 7 bits we need to 'split' it up.
            while (value > 0b0111_1111)
            {
                // Write out the value and set the 8th bit to 1 to indicate more data will follow.
                WriteByte(ref span, (byte)(value | 0b1000_0000));

                // Shift the value by 7 to 'consume' the bits we've just written.
                value >>= 7;
            }

            // Write out the last data (the 8th bit will always be 0 here to indicate the end).
            WriteByte(ref span, (byte)value);
        }

        /// <summary>
        /// Write a boolean.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteBool(ref Span<byte> span, bool val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(bool));
        }

        /// <summary>
        /// Write a unsigned 8 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteByte(ref Span<byte> span, byte val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(byte));
        }

        /// <summary>
        /// Write a signed 8 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteSByte(ref Span<byte> span, sbyte val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(sbyte));
        }

        /// <summary>
        /// Write a 16 bit signed integer.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteShort(ref Span<byte> span, short val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(short));
        }

        /// <summary>
        /// Write a 16 bit unsigned integer.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteUShort(ref Span<byte> span, ushort val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(ushort));
        }

        /// <summary>
        /// Write a 32 bit signed integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteInt(ref Span<byte> span, int val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(int));
        }

        /// <summary>
        /// Write a 32 bit unsigned integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteUInt(ref Span<byte> span, uint val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(uint));
        }

        /// <summary>
        /// Write a 64 bit signed integer.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteLong(ref Span<byte> span, long val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(long));
        }

        /// <summary>
        /// Write a 64 bit unsigned integer.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteULong(ref Span<byte> span, ulong val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(ulong));
        }

        /// <summary>
        /// Write a 32 bit floating-point number.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteFloat(ref Span<byte> span, float val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(float));
        }

        /// <summary>
        /// Write a 64 bit floating-point number.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteDouble(ref Span<byte> span, double val)
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            span = span.Slice(sizeof(double));
        }

        /// <summary>
        /// Write a value as a fraction between a given minimum and maximum.
        /// Uses 8 bits so we have '256' steps between min and max.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="min">Minimum value of the range.</param>
        /// <param name="max">Maximum value of the range.</param>
        /// <param name="val">Value to write (within the range.</param>
        public static void Write8BitRange(ref Span<byte> span, float min, float max, float val)
        {
            // Get a 0f - 1f fraction.
            var frac = Fraction(min, max, val);

            // Remap it to a byte and write it (+ .5f because we want round, not floor).
            BinSerialize.WriteByte(ref span, (byte)((byte.MaxValue * frac) + .5f));
        }

        /// <summary>
        /// Write a value as a fraction between a given minimum and maximum.
        /// Uses 16 bits so we have '65535' steps between min and max.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="min">Minimum value of the range.</param>
        /// <param name="max">Maximum value of the range.</param>
        /// <param name="val">Value to write (within the range.</param>
        public static void Write16BitRange(ref Span<byte> span, float min, float max, float val)
        {
            // Get a 0f - 1f fraction.
            var frac = Fraction(min, max, val);

            // Remap it to a ushort and write it (+ .5f because we want round, not floor).
            BinSerialize.WriteUShort(ref span, (ushort)((ushort.MaxValue * frac) + .5f));
        }

        /// <summary>
        /// Write a continuous block of bytes.
        /// </summary>
        /// <remarks>
        /// Will consume as many bytes as are in the given block.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Block of bytes to write.</param>
        public static void WriteBlock(ref Span<byte> span, ReadOnlySpan<byte> val)
        {
            val.CopyTo(span);

            // 'Advance' the span.
            span = span.Slice(val.Length);
        }

        /// <summary>
        /// Check how many many bytes it will take to write the given string value.
        /// </summary>
        /// <remarks>
        /// Size will be the length of the string as a 'packed unsigned integer' + the amount of
        /// bytes when the characters are utf-8 encoded.
        /// </remarks>
        /// <param name="val">Value to get the size for.</param>
        /// <returns>Number of bytes it will take.</returns>
        public static int GetSizeForString(string val)
        {
            fixed (char* charPointer = val)
            {
                return GetSizeForString(charPointer, val.Length);
            }
        }

        /// <summary>
        /// Check how many many bytes it will take to write the given string.
        /// </summary>
        /// <remarks>
        /// Size will be the length of the span as a 'packed unsigned integer' + the amount of
        /// bytes when the characters are utf-8 encoded.
        /// </remarks>
        /// <param name="val">Value to get the size for.</param>
        /// <returns>Number of bytes it will take.</returns>
        public static int GetSizeForString(ReadOnlySpan<char> val)
        {
            fixed (char* charPointer = val)
            {
                return GetSizeForString(charPointer, val.Length);
            }
        }

        /// <summary>
        /// Check how many many bytes it will take to write the given string.
        /// Make sure the data behind the pointer is pinned before calling this.
        /// </summary>
        /// <remarks>
        /// Size will be the charCount as a 'packed unsigned integer' + the amount of
        /// bytes when the characters are utf-8 encoded.
        /// </remarks>
        /// <param name="charPointer">Pointer to the first character.</param>
        /// <param name="charPointer">How many characters are in the string.</param>
        /// <returns>Number of bytes it will take.</returns>
        public static int GetSizeForString(char* charPointer, int charCount)
        {
            var headerSize = GetSizeForPackedUnsignedInteger((uint)charCount);
            var charsSize = uft8.GetByteCount(charPointer, charCount);
            return headerSize + charsSize;
        }

        /// <summary>
        /// Write a string as utf8.
        /// </summary>
        /// <remarks>
        /// Prefixes the data with a 'packed unsigned integer' telling how many bytes will follow.
        /// Format will match that of a <see cref="System.IO.BinaryWriter"/> that is using utf8 encoding.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteString(ref Span<byte> span, string val)
        {
            fixed (char* charPointer = val)
            {
                WriteString(ref span, charPointer, val.Length);
            }
        }

        /// <summary>
        /// Write a string as utf8.
        /// </summary>
        /// <remarks>
        /// Prefixes the data with a 'packed unsigned integer' telling how many bytes will follow.
        /// Format will match that of a <see cref="System.IO.BinaryWriter"/> that is using utf8 encoding.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        public static void WriteString(ref Span<byte> span, ReadOnlySpan<char> val)
        {
            fixed (char* charPointer = val)
            {
                WriteString(ref span, charPointer, val.Length);
            }
        }

        /// <summary>
        /// Write a string as utf8.
        /// Make sure the data behind the pointer is pinned before calling this.
        /// </summary>
        /// <remarks>
        /// Prefixes the data with a 'packed unsigned integer' telling how many bytes will follow.
        /// Format will match that of a <see cref="System.IO.BinaryWriter"/> that is using utf8 encoding.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="charPointer">Pointer to the first character.</param>
        /// <param name="charPointer">How many characters are in the string.</param>
        public static void WriteString(ref Span<byte> span, char* charPointer, int charCount)
        {
            // Write amount of bytes will follow.
            var byteCount = uft8.GetByteCount(charPointer, charCount);
            WritePackedUnsignedInteger(ref span, (uint)byteCount);

            fixed (byte* spanPointer = span)
            {
                // Write chars as utf8.
                var writtenBytes = uft8.GetBytes(charPointer, charCount, spanPointer, span.Length);
                Debug.Assert(byteCount == writtenBytes, "Written bytes did not match encodings expected size");
            }

            // 'Advance' the span.
            span = span.Slice(byteCount);
        }

        /// <summary>
        /// Write a unmanaged struct.
        /// </summary>
        /// <remarks>
        /// When using this make sure that 'T' has a explict memory-layout so its consistent
        /// accross platforms.
        /// In other words, only use this if you are 100% sure its safe to do so.
        /// Will consume sizeof T.
        /// </remarks>
        /// <param name="span">Span to write to.</param>
        /// <param name="val">Value to write.</param>
        /// <typeparam name="T">Type of the struct to write.</typeparam>
        public static void WriteStruct<T>(ref Span<byte> span, T val)
            where T : unmanaged
        {
            MemoryMarshal.Write(span, ref val);

            // 'Advance' the span.
            var size = Unsafe.SizeOf<T>();
            span = span.Slice(size);
        }

        /// <summary>
        /// Read a boolean.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static bool ReadBool(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<bool>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(bool));

            return result;
        }

        /// <summary>
        /// Read a unsigned 8 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static byte ReadByte(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<byte>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(byte));

            return result;
        }

        /// <summary>
        /// Read a signed 8 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static sbyte ReadSByte(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<sbyte>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(sbyte));

            return result;
        }

        /// <summary>
        /// Read a signed 16 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static short ReadShort(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<short>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(short));

            return result;
        }

        /// <summary>
        /// Read a unsigned 16 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static ushort ReadUShort(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<ushort>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(ushort));

            return result;
        }

        /// <summary>
        /// Read a signed 32 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static int ReadInt(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<int>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(int));

            return result;
        }

        /// <summary>
        /// Read a signed 32 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static int ReadInt32BigEndian(ref ReadOnlySpan<byte> span)
        {
            var result = BinaryPrimitives.ReadInt32BigEndian(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(int));

            return result;
        }

        /// <summary>
        /// Read a unsigned 32 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static uint ReadUInt(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<uint>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(uint));

            return result;
        }

        /// <summary>
        /// Read a signed 64 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static long ReadLong(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<long>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(long));

            return result;
        }

        /// <summary>
        /// Read a unsigned 64 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static ulong ReadULong(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<ulong>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(ulong));

            return result;
        }

        /// <summary>
        /// Read a 32 bit floating-point number.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static float ReadFloat(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<float>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(float));

            return result;
        }

        /// <summary>
        /// Read a 64 bit floating-point number.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read value.</returns>
        public static double ReadDouble(ref ReadOnlySpan<byte> span)
        {
            var result = MemoryMarshal.Read<double>(span);

            // 'Advance' the span.
            span = span.Slice(sizeof(double));

            return result;
        }

        /// <summary>
        /// Read a value as a fraction between a given minimum and maximum.
        /// Uses 8 bits so we have '256' steps between min and max.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <param name="min">Minimum value of the range.</param>
        /// <param name="max">Maximum value of the range.</param>
        /// <returns>Read value.</returns>
        public static float Read8BitRange(ref ReadOnlySpan<byte> span, float min, float max)
        {
            // Read a byte.
            var raw = BinSerialize.ReadByte(ref span);

            // Remap it to the given range.
            return Interpolate(min, max, (float)raw / byte.MaxValue);
        }

        /// <summary>
        /// Read a value as a fraction between a given minimum and maximum.
        /// Uses 16 bits so we have '65535' steps between min and max.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <param name="min">Minimum value of the range.</param>
        /// <param name="max">Maximum value of the range.</param>
        /// <returns>Read value.</returns>
        public static float Read16BitRange(ref ReadOnlySpan<byte> span, float min, float max)
        {
            // Read a ushort.
            var raw = BinSerialize.ReadUShort(ref span);

            // Remap it to the given range.
            return Interpolate(min, max, (float)raw / ushort.MaxValue);
        }


        public static string ReadProtoString(ref ReadOnlySpan<byte> span)
        {
            if (2 != (ReadByte(ref span) & 0b111))
                throw new Exception();
            return ReadString(ref span);
        }

        public static uint ReadProtoUInt32(ref ReadOnlySpan<byte> span)
        {
            if (0 != (ReadByte(ref span) & 0b111))
                throw new Exception();
            return ReadPackedUnsignedInteger(ref span);
        }
        public static (int index, int size) ReadProtoByteArraySize(ref ReadOnlySpan<byte> span)
        {
            var header = ReadByte(ref span);
            if (2 != (header & 0b111))
                throw new Exception();
            return (header >> 3, (int)ReadPackedUnsignedInteger(ref span));
        }

        /// <summary>
        /// Read a string.
        /// </summary>
        /// <remarks>
        /// Can only be used for strings less then 128 kib as utf8, for bigger strings use a overload
        /// where you pass a 'Span{char}' as the output buffer.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <returns>Read string.</returns>
        public static string ReadString(ref ReadOnlySpan<byte> span)
        {
            const int MaxStackStringBytes = 128 * 1024; // 128 KiB.

            // Read how many bytes will follow.
            var byteCount = (int)ReadPackedUnsignedInteger(ref span);

            // Check if the span contains the entire string.
            if (span.Length < byteCount)
                throw new ArgumentOutOfRangeException(nameof(span), "Given span is incomplete");

            // Sanity check the size before allocating space on the stack.
            if (byteCount >= MaxStackStringBytes)
                throw new ArgumentException("Input contains a string with too many bytes to fit on the stack", nameof(span));

            // Decode on the stack to avoid having to allocate a temporary buffer on the heap.
            var maxCharCount = uft8.GetMaxCharCount(byteCount);
            var charBuffer = stackalloc char[maxCharCount];

            // Read chars as utf8.
            int actualCharCount;
            fixed (byte* bytePointer = span)
            {
                actualCharCount = utf8decoder.GetChars(bytePointer, byteCount, charBuffer, maxCharCount, flush: false);
            }

            // 'Advance' the span.
            span = span.Slice(byteCount);

            // Allocate the string.
            return new string(charBuffer, startIndex: 0, length: actualCharCount);
        }

        /// <summary>
        /// Read a string to a given output-buffer.
        /// </summary>
        /// <param name="span">Span to read from.</param>
        /// <param name="chars">Buffer to write to.</param>
        /// <returns>Amount of characters written</returns>
        public static int ReadString(ref ReadOnlySpan<byte> span, Span<char> chars)
        {
            // Read amount of bytes will follow.
            var byteCount = (int)ReadPackedUnsignedInteger(ref span);

            // Check if input span contains the entire string.
            if (span.Length < byteCount)
                throw new ArgumentOutOfRangeException(nameof(span), "Given span is incomplete");

            // No need to check if the output span has enough space as 'Encoding.GetChars' will
            // already do that for us.

            // Read chars as utf8.
            int charsRead;
            fixed (char* charPointer = chars)
            fixed (byte* bytePointer = span)
            {
                charsRead = uft8.GetChars(bytePointer, byteCount, charPointer, chars.Length);
            }

            // 'Advance' the span.
            span = span.Slice(byteCount);

            return charsRead;
        }

        /// <summary>
        /// Read a unmanaged struct.
        /// </summary>
        /// <remarks>
        /// When using this make sure that 'T' has a explict memory-layout so its consistent
        /// accross platforms.
        /// In other words, only use this if you are 100% sure its safe to do so.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <typeparam name="T">Type of the struct to read.</typeparam>
        /// <returns>Read value.</returns>
        public static T ReadStruct<T>(ref ReadOnlySpan<byte> span)
            where T : unmanaged
        {
            var result = MemoryMarshal.Read<T>(span);

            // 'Advance' the span.
            var size = Unsafe.SizeOf<T>();
            span = span.Slice(size);

            return result;
        }

        /// <summary>
        /// Read a continuous block of bytes as a new byte-array.
        /// </summary>
        /// <remarks>
        /// Will consume '<paramref name="byteCount"/>' amount of bytes.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <param name="byteCount">Amount of bytes to read.</param>
        /// <returns>New byte-array containing the read bytes.</returns>
        public static byte[] ReadBlock(ref ReadOnlySpan<byte> span, int byteCount)
        {
            var result = new byte[byteCount];
            ReadBlock(ref span, result);
            return result;
        }

        /// <summary>
        /// Read a continuous block of bytes into given output span.
        /// </summary>
        /// <remarks>
        /// Will consume length of '<paramref name="output"/>'.
        /// </remarks>
        /// <param name="span">Span to read from.</param>
        /// <param name="output">Span to write to.</param>
        public static void ReadBlock(ref ReadOnlySpan<byte> span, Span<byte> output)
        {
            span.Slice(0, output.Length).CopyTo(output);

            // 'Advance' the span.
            span = span.Slice(output.Length);
        }

        /// <summary>
        /// 'Reserve' space for a boolean and return a ref to the space.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref bool ReserveBool(ref Span<byte> span)
        {
            ref bool result = ref Unsafe.As<byte, bool>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(bool));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a unsigned 8 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref byte ReserveByte(ref Span<byte> span)
        {
            ref byte result = ref span[0];

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(byte));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a signed 8 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 1 byte.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref sbyte ReserveSByte(ref Span<byte> span)
        {
            ref sbyte result = ref Unsafe.As<byte, sbyte>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(sbyte));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a signed 16 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref short ReserveShort(ref Span<byte> span)
        {
            ref short result = ref Unsafe.As<byte, short>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(short));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a unsigned 16 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 2 bytes.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref ushort ReserveUShort(ref Span<byte> span)
        {
            ref ushort result = ref Unsafe.As<byte, ushort>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(ushort));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a signed 32 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref int ReserveInt(ref Span<byte> span)
        {
            ref int result = ref Unsafe.As<byte, int>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(int));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a unsigned 32 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 4 bytes.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref uint ReserveUInt(ref Span<byte> span)
        {
            ref uint result = ref Unsafe.As<byte, uint>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(uint));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a signed 64 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref long ReserveLong(ref Span<byte> span)
        {
            ref long result = ref Unsafe.As<byte, long>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(long));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a unsigned 64 bit integer.
        /// </summary>
        /// <remarks>
        /// Will consume 8 bytes.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <returns>Reference to the reserved space.</returns>
        public static ref ulong ReserveULong(ref Span<byte> span)
        {
            ref ulong result = ref Unsafe.As<byte, ulong>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            span = span.Slice(sizeof(ulong));

            return ref result;
        }

        /// <summary>
        /// 'Reserve' space for a unmanaged struct.
        /// </summary>
        /// <remarks>
        /// When using this make sure that 'T' has a explict memory-layout so its consistent
        /// accross platforms.
        /// In other words, only use this if you are 100% sure its safe to do so.
        /// Will consume sizeof T.
        /// </remarks>
        /// <param name="span">Span to reserver from.</param>
        /// <typeparam name="T">Type of the unmanaged struct.</typeparam>
        /// <returns>Reference to the reserved space.</returns>
        public static ref T ReserveStruct<T>(ref Span<byte> span)
            where T : unmanaged
        {
            ref T result = ref Unsafe.As<byte, T>(ref span[0]);

            // Init to default, as otherwise it would be whatever data was at that memory.
            result = default;

            // 'Advance' the span.
            var size = Unsafe.SizeOf<T>();
            span = span.Slice(size);

            return ref result;
        }

        private static uint ToZigZagEncoding(int val)
        {
            /* We encode the integer in such a way that the sign is on the least significant bit,
            known as zig-zag encoding:
            https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding */

            return (uint)((val << 1) ^ (val >> 31));
        }

        private static long FromZigZagEncoding(ulong zigzagged)
        {
            /* We encode integers in such a way that the sign is on the least significant bit,
            known as zig-zag encoding:
            https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding */

            return (long)(zigzagged >> 1) ^ -(long)(zigzagged & 1);
        }

        private static int FromZigZagEncoding(uint zigzagged)
        {
            /* We encode integers in such a way that the sign is on the least significant bit,
            known as zig-zag encoding:
            https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding */

            return (int)(zigzagged >> 1) ^ -(int)(zigzagged & 1);
        }

        private static float Interpolate(float min, float max, float frac) =>
            min + ((max - min) * Clamp01(frac));

        private static float Fraction(float min, float max, float val) =>
            (min == max) ? 0f : Clamp01((val - min) / (max - min));

        private static float Clamp01(float val) => val < 0f ? 0f : (val > 1f ? 1f : val);
    }
}
