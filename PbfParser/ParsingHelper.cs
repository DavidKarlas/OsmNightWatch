﻿using LibDeflate;
using System.Buffers.Binary;
using System.Buffers;
using System.Collections.Concurrent;

namespace OsmNightWatch.PbfParsing
{
    public static partial class ParsingHelper
    {
        public static int ParseHeader(Span<byte> buffer, FileStream file, string expectedHeader)
        {
            Span<byte> headerSizeBuffer = buffer.Slice(0, 4);
            if (file.Read(headerSizeBuffer) != headerSizeBuffer.Length)
                return 0;

            int blobHeaderSize = BinaryPrimitives.ReadInt32BigEndian(headerSizeBuffer);
            var osmHeaderBuffer = buffer.Slice(0, blobHeaderSize);
            if (file.Read(osmHeaderBuffer) != osmHeaderBuffer.Length)
                throw new Exception("File too small.");
            ReadOnlySpan<byte> osmHeaderBufferR = osmHeaderBuffer;
            string headerType = BinSerialize.ReadProtoString(ref osmHeaderBufferR);
            if (headerType != expectedHeader)
                throw new Exception(headerType);
            return (int)BinSerialize.ReadProtoUInt32(ref osmHeaderBufferR);
        }

        public static void ParallelParse(string path, List<(long FileOffset, HashSet<long>? AllElementsInside)> fileOffsets, Action<HashSet<long>?, byte[], object?> action, Func<object>? perThreadStateCreator = null)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            var tasks = new List<Task>();
            var slimSemaphore = new SemaphoreSlim(24);
            var states = new ConcurrentStack<object?>(Enumerable.Range(0, slimSemaphore.CurrentCount).Select((i) => perThreadStateCreator == null ? null : perThreadStateCreator()));
            foreach (var fileOffset in fileOffsets)
            {
                file.Seek(fileOffset.FileOffset, SeekOrigin.Begin);
                var readBuffer = ArrayPool<byte>.Shared.Rent(16 * 1024 * 1024);
                var blobSize = ParseHeader(readBuffer, file, "OSMData");
                if (blobSize == 0)
                    break;
                if (file.Read(readBuffer, 0, blobSize) != blobSize)
                    throw new Exception("Too small file.");
                slimSemaphore.Wait();
                tasks.Add(Task.Run(() => {
                    if (!states.TryPop(out var state))
                    {
                        throw new InvalidOperationException();
                    }
                    action(fileOffset.AllElementsInside, readBuffer, state);
                    ArrayPool<byte>.Shared.Return(readBuffer);
                    states.Push(state);
                    slimSemaphore.Release();
                }));
                tasks.RemoveAll(task => task.IsCompleted);
            }
            Task.WhenAll(tasks).Wait();
        }

        public static void Decompress(ref ReadOnlySpan<byte> readDataR, out byte[] uncompressbuffer, out ReadOnlySpan<byte> uncompressedData, out uint uncompressedDataSize)
        {
            uncompressedDataSize = BinSerialize.ReadProtoUInt32(ref readDataR);
            var compressedDataSize = BinSerialize.ReadProtoByteArraySize(ref readDataR).size;
            uncompressbuffer = ArrayPool<byte>.Shared.Rent(16 * 1024 * 1024);
            using var decompressor = new ZlibDecompressor();
            var state = decompressor.Decompress(readDataR.Slice(0, compressedDataSize), uncompressbuffer, out int written);
            if (uncompressedDataSize != written)
            {
                throw new Exception();
            }
            uncompressedData = new ReadOnlySpan<byte>(uncompressbuffer, 0, (int)uncompressedDataSize);
        }
    }
}
