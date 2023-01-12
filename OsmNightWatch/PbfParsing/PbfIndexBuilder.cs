using LibDeflate;
using OsmSharp;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch.PbfParsing
{
    public class PbfIndexBuilder
    {
        public static PbfIndex BuildIndex(string pbfPath, bool ignoreCache = false)
        {
            var cachePath = Path.ChangeExtension(pbfPath, "pbf.index");
            if (!ignoreCache && File.Exists(cachePath))
            {
                using var cacheReadStream = File.Open(cachePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                Console.WriteLine("Loaded PbfIndex from cache file.");
                return new PbfIndex(cacheReadStream, pbfPath);
            }
            Console.WriteLine("Building PbfIndex...");
            var file = File.Open(pbfPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var slimSemaphore = new SemaphoreSlim(24);
            ReadHeader(file);
            var nodesBag = new ConcurrentBag<(long FirstNodeId, long FileOffset)>();
            var waysBag = new ConcurrentBag<(long FirstWayId, long FileOffset)>();
            var relationsBag = new ConcurrentBag<(long FirstRelationId, long FileOffset)>();
            var tasks = new List<Task>();
            while (true)
            {
                var readBuffer = ArrayPool<byte>.Shared.Rent(16 * 1024 * 1024);
                var fileOffset = file.Position;
                var blobSize = ParseHeader(readBuffer, file, "OSMData");
                if (blobSize == 0)
                    break;
                if (file.Read(readBuffer, 0, blobSize) != blobSize)
                    throw new Exception("Too small file.");
                slimSemaphore.Wait();
                tasks.Add(Task.Factory.StartNew(ProcessBlob, (fileOffset, nodesBag, waysBag, relationsBag, readBuffer, slimSemaphore)));
                tasks.RemoveAll(task => task.IsCompleted);
            }
            Task.WhenAll(tasks).Wait();
            var index = new PbfIndex(
                pbfPath,
                nodesBag.OrderBy(n => n.FirstNodeId).ToArray(),
                waysBag.OrderBy(n => n.FirstWayId).ToArray(),
                relationsBag.OrderBy(n => n.FirstRelationId).ToArray());
            using var cacheWriteStream = File.Open(cachePath, FileMode.Create);
            index.Serialize(cacheWriteStream);
            Console.WriteLine("Finished building PbfIndex.");
            return index;
        }

        static void ProcessBlob(object? stateObj)
        {
            if (stateObj == null)
                return;
            var state = ((
                long fileOffset,
                ConcurrentBag<(long FirstNodeId, long FileOffset)> nodesBag,
                ConcurrentBag<(long FirstWayId, long FileOffset)> waysBag,
                ConcurrentBag<(long FirstRelationId, long FileOffset)> relationsBag,
                byte[] readBuffer,
                SemaphoreSlim slimSemaphore))stateObj;
            var firstElement = ParseBlob(state.readBuffer);
            switch (firstElement.Type)
            {
                case OsmGeoType.Node:
                    state.nodesBag.Add((firstElement.Id, state.fileOffset));
                    break;
                case OsmGeoType.Way:
                    state.waysBag.Add((firstElement.Id, state.fileOffset));
                    break;
                case OsmGeoType.Relation:
                    state.relationsBag.Add((firstElement.Id, state.fileOffset));
                    break;
            }
            ArrayPool<byte>.Shared.Return(state.readBuffer);
            state.slimSemaphore.Release();
        }

        static int ParseHeader(Span<byte> buffer, FileStream file, string expectedHeader)
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

        static void ReadHeader(FileStream file)
        {
            Span<byte> readBuffer = stackalloc byte[16 * 1024];
            var headerBlobSize = ParseHeader(readBuffer, file, "OSMHeader");
            file.Seek(headerBlobSize, SeekOrigin.Current);
        }

        static (OsmGeoType Type, long Id) ParseBlob(byte[] readBuffer)
        {
            ReadOnlySpan<byte> readDataR = readBuffer;
            var uncompressedDataSize = BinSerialize.ReadProtoUInt32(ref readDataR);
            var compressedDataSize = BinSerialize.ReadProtoByteArraySize(ref readDataR).size;
            var uncompressbuffer = ArrayPool<byte>.Shared.Rent(16 * 1024 * 1024);
            using Decompressor decompressor = new ZlibDecompressor();
            decompressor.Decompress(readDataR.Slice(0, compressedDataSize), uncompressbuffer, out int written);
            if (uncompressedDataSize != written)
            {
                throw new Exception();
            }
            var uncompressedData = new ReadOnlySpan<byte>(uncompressbuffer, 0, (int)uncompressedDataSize);
            var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedData).size;
            uncompressedData = uncompressedData.Slice(stringTableSize);
            //var targetLength = uncompressedData.Length - stringTableSize;
            //while (uncompressedData.Length > targetLength)
            //{
            //    var size = BinSerialize.ReadProtoString(ref uncompressedData);
            //}
            var primitivegroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedData).size;
            var lengthAtEnd = uncompressedData.Length - primitivegroupSize;
            while (uncompressedData.Length > lengthAtEnd)
            {
                var (index, primitiveSize) = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
                switch (index)
                {
                    case 2:
                        var lengthAtEndDenseNodes = uncompressedData.Length - primitiveSize;
                        var sizeOfIds = BinSerialize.ReadProtoByteArraySize(ref uncompressedData).size;
                        var targetLength = uncompressedData.Length - sizeOfIds;
                        long currentNodeId = 0;
                        while (uncompressedData.Length > targetLength)
                        {
                            currentNodeId += BinSerialize.ReadZigZagLong(ref uncompressedData);
                            ArrayPool<byte>.Shared.Return(uncompressbuffer);
                            return (OsmGeoType.Node, currentNodeId);// return first node since right now we only care about that
                        }
                        uncompressedData = uncompressedData.Slice(uncompressedData.Length - lengthAtEndDenseNodes);

                        //if (uncompressedData.Length == 0)
                        //    return -1;
                        primitivegroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedData).size;
                        lengthAtEnd = uncompressedData.Length - primitivegroupSize;
                        break;
                    case 3:
                        BinSerialize.EnsureProtoIndexAndType(ref uncompressedData, 1, 0);
                        var wayid = BinSerialize.ReadPackedLong(ref uncompressedData);
                        ArrayPool<byte>.Shared.Return(uncompressbuffer);
                        return (OsmGeoType.Way, wayid);
                    case 4:
                        BinSerialize.EnsureProtoIndexAndType(ref uncompressedData, 1, 0);
                        var relationId = BinSerialize.ReadPackedLong(ref uncompressedData);
                        ArrayPool<byte>.Shared.Return(uncompressbuffer);
                        return (OsmGeoType.Relation, relationId);
                }
            }
            throw new Exception();
        }
    }

    public class PbfIndex
    {
        private readonly (long FirstNodeId, long FileOffset)[] nodeOffsets;
        private readonly (long FirstWayId, long FileOffset)[] wayOffsets;
        private readonly (long FirstRelationId, long FileOffset)[] relationOffsets;
        public string PbfPath { get; }

        public PbfIndex(
            string path,
            (long FirstNodeId, long FileOffset)[] nodeOffsets,
            (long FirstWayId, long FileOffset)[] wayOffsets,
            (long FirstRelationId, long FileOffset)[] relationOffsets)
        {
            PbfPath = path;
            this.nodeOffsets = nodeOffsets;
            this.wayOffsets = wayOffsets;
            this.relationOffsets = relationOffsets;
        }

        public PbfIndex(Stream cacheStream, string pbfPath)
        {
            var binaryReader = new BinaryReader(cacheStream);
            nodeOffsets = new (long FirstNodeId, long FileOffset)[binaryReader.ReadInt32()];
            long maxSize = 0;
            for (int i = 0; i < nodeOffsets.Length; i++)
            {
                nodeOffsets[i] = (binaryReader.ReadInt64(), binaryReader.ReadInt64());
                if (i > 1)
                {
                    maxSize = Math.Max(maxSize, nodeOffsets[i].FileOffset - nodeOffsets[i - 1].FileOffset);
                }
            }

            wayOffsets = new (long FirstWayId, long FileOffset)[binaryReader.ReadInt32()];
            for (int i = 0; i < wayOffsets.Length; i++)
            {
                wayOffsets[i] = (binaryReader.ReadInt64(), binaryReader.ReadInt64());
                if (i > 1)
                {
                    maxSize = Math.Max(maxSize, wayOffsets[i].FileOffset - wayOffsets[i - 1].FileOffset);
                }
            }

            relationOffsets = new (long FirstRelationId, long FileOffset)[binaryReader.ReadInt32()];
            for (int i = 0; i < relationOffsets.Length; i++)
            {
                relationOffsets[i] = (binaryReader.ReadInt64(), binaryReader.ReadInt64());
                if (i > 1)
                {
                    maxSize = Math.Max(maxSize, relationOffsets[i].FileOffset - relationOffsets[i - 1].FileOffset);
                }
            }
            PbfPath = pbfPath;
        }

        public void Serialize(Stream stream)
        {
            var binaryWriter = new BinaryWriter(stream);
            binaryWriter.Write(nodeOffsets.Length);
            foreach (var node in nodeOffsets)
            {
                binaryWriter.Write(node.FirstNodeId);
                binaryWriter.Write(node.FileOffset);
            }

            binaryWriter.Write(wayOffsets.Length);
            foreach (var way in wayOffsets)
            {
                binaryWriter.Write(way.FirstWayId);
                binaryWriter.Write(way.FileOffset);
            }

            binaryWriter.Write(relationOffsets.Length);
            foreach (var relation in relationOffsets)
            {
                binaryWriter.Write(relation.FirstRelationId);
                binaryWriter.Write(relation.FileOffset);
            }
        }

        public long GetNodeFileOffset(long nodeId)
        {
            return BinarySearch(nodeOffsets, nodeId);
        }
        public long GetWayFileOffset(long wayId)
        {
            return BinarySearch(wayOffsets, wayId);
        }
        public long GetRelationFileOffset(long relationId)
        {
            return BinarySearch(relationOffsets, relationId);
        }

        public long GetFirstNodeOffset()
        {
            return nodeOffsets[0].FileOffset;
        }

        public long GetFirstWayOffset()
        {
            return wayOffsets[0].FileOffset;
        }

        public long GetFirstRelationOffset()
        {
            return relationOffsets[0].FileOffset;
        }

        static long BinarySearch((long Id, long Offset)[] array, long elementId)
        {
            int first = 0;
            int last = array.Length - 1;
            int mid;
            do
            {
                mid = first + (last - first) / 2;
                if (elementId > array[mid].Id)
                    first = mid + 1;
                else
                    last = mid - 1;
                if (elementId == array[mid].Id)
                    return mid;
            } while (first <= last);
            return array[last].Offset;
        }

        public List<long> GetAllNodeFileOffsets()
        {
            return nodeOffsets.Select(o => o.FileOffset).ToList();
        }
        public List<long> GetAllWayFileOffsets()
        {
            return wayOffsets.Select(o => o.FileOffset).ToList();
        }
        public List<long> GetAllRelationFileOffsets()
        {
            return relationOffsets.Select(o => o.FileOffset).ToList();
        }

        public List<(long FileOffset, HashSet<long>? AllElementsInside)> CaclulateFileOffsets(IEnumerable<long> elementsToLoad, OsmGeoType type)
        {
            var result = new List<(long FileOffset, HashSet<long>?)>();
            var sortedElements = elementsToLoad.ToArray();
            Array.Sort(sortedElements);

            (long Id, long Offset)[] offsets = type switch
            {
                OsmGeoType.Node => nodeOffsets,
                OsmGeoType.Way => wayOffsets,
                OsmGeoType.Relation => relationOffsets,
                _ => throw new NotImplementedException()
            };

            var currentIndex = 0;
            for (int i = 0; i < offsets.Length; i++)
            {
                var startId = offsets[i].Id;
                var endId = i + 1 < offsets.Length ? offsets[i + 1].Id : long.MaxValue;

                (long FileOffset, HashSet<long> Ids) newBucket = (offsets[i].Offset, new HashSet<long>());

                for (int j = currentIndex; j < sortedElements.Length; j++)
                {
                    var id = sortedElements[j];
                    if (startId <= id && endId > id)
                    {
                        newBucket.Ids.Add(id);
                    }
                    else
                    {
                        break;
                    }
                }
                if (newBucket.Ids.Count > 0)
                {
                    currentIndex += newBucket.Ids.Count;
                    result.Add(newBucket);
                }
            }
            return result;
        }

        public long GetLastNodeOffset()
        {
            return nodeOffsets.Last().FileOffset;
        }
    }
}
