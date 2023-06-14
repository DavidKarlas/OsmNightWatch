using OsmSharp;
using OsmSharp.Tags;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using static OsmNightWatch.PbfParsing.ParsingHelper;

namespace OsmNightWatch.PbfParsing
{
    public static class WaysParser
    {
        public static IReadOnlyCollection<Way> LoadWays(HashSet<long>? waysToLoad, PbfIndex index)
        {
            if (waysToLoad == null || waysToLoad.Count == 0)
                return Array.Empty<Way>();
            var fileOffsets = index.CalculateFileOffsets(waysToLoad, OsmGeoType.Way);
            var waysBag = new ConcurrentBag<Way>();
            ParallelParse(index.PbfPath, fileOffsets, (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                ParseWays(relevantIds, (way) => {
                    waysBag.Add(way);
                }, null, readBuffer);
            });

            return waysBag;
        }

        public static IEnumerable<Way> Parse(IEnumerable<ElementFilter> filters, PbfIndex index)
        {
            var indexFilters = new IndexedTagFilters(filters.Where(f => f.GeoType == OsmGeoType.Way).SelectMany(f => f.Tags));
            var waysQueue = new ConcurrentQueue<List<Way>>();
            var task = Task.Run(() => ParallelParse(index.PbfPath, index.GetAllWayFileOffsets().Select(o => (o, (HashSet<long>?)null)).ToList(),
                (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                    var waysBag = new List<Way>();
                    ParseWays(null, (way) => {
                        waysBag.Add(way);
                    }, indexFilters, readBuffer);
                    waysQueue.Enqueue(waysBag);
                }));
            while (task.IsCompleted == false || waysQueue.Count > 0)
            {
                while (waysQueue.TryDequeue(out var waysBag))
                {
                    foreach (var way in waysBag)
                    {
                        yield return way;
                    }
                }
            }
        }
        public static void Process(Action<Way> action, IEnumerable<ElementFilter> filters, PbfIndex index)
        {
            var indexFilters = new IndexedTagFilters(filters.Where(f => f.GeoType == OsmGeoType.Way).SelectMany(f => f.Tags));
            ParallelParse(index.PbfPath, index.GetAllWayFileOffsets().Select(o => (o, (HashSet<long>?)null)).ToList(),
                (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                    ParseWays(null, action, indexFilters, readBuffer);
                });
        }

        public static void ParseWaysNodes(PbfIndex index, List<(long FileOffset, HashSet<long>? AllElementsInside)> fileOffsets, Func<IWayNodesCallback> callback)
        {
            ParallelParse(index.PbfPath, fileOffsets,
                (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                    ReadOnlySpan<byte> dataSpan = readBuffer;
                    Decompress(ref dataSpan, out var uncompressbuffer, out var uncompressedSpan, out var uncompressedSize);
                    var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
                    uncompressedSpan = uncompressedSpan.Slice(stringTableSize);
                    while (uncompressedSpan.Length > 0)
                    {
                        var primitiveGroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
                        var expectedLengthAtEndOfPrimitiveGroup = uncompressedSpan.Length - primitiveGroupSize;
                        while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitiveGroup)
                        {
                            var (index, primitiveSize) = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan);
                            var expectedLengthAtEndOfPrimitive = uncompressedSpan.Length - primitiveSize;
                            if (index != 3)
                            {
                                throw new Exception();//Only expecting Ways here
                            }
                            BinSerialize.EnsureProtoIndexAndType(ref uncompressedSpan, 1, 0);
                            var wayId = BinSerialize.ReadPackedLong(ref uncompressedSpan);
                            if (!relevantIds!.Contains(wayId))
                            {
                                uncompressedSpan = uncompressedSpan.Slice(uncompressedSpan.Length - expectedLengthAtEndOfPrimitive);
                                continue;
                            }
                            while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitive)
                            {
                                var (innerIndex, type) = BinSerialize.ReadProtoIndexAndType(ref uncompressedSpan);
                                switch (innerIndex)
                                {
                                    case 2://Tag keys
                                        var sizeOfKeys = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        uncompressedSpan = uncompressedSpan.Slice(sizeOfKeys);
                                        break;
                                    case 3://Tag values
                                        var sizeOfValues = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        uncompressedSpan = uncompressedSpan.Slice(sizeOfValues);
                                        break;
                                    case 4://Info
                                        var sizeOfInfo = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        uncompressedSpan = uncompressedSpan.Slice(sizeOfInfo);
                                        break;
                                    case 8://node ids
                                        var sizeOfRefs = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        var expectedLengthAtEndOfRefs = uncompressedSpan.Length - sizeOfRefs;
                                        long nodeId = 0;
                                        while (uncompressedSpan.Length > expectedLengthAtEndOfRefs)
                                        {
                                            nodeId += BinSerialize.ReadZigZagLong(ref uncompressedSpan);
                                            ((IWayNodesCallback)state!).Invoke(wayId, nodeId);
                                        }
                                        break;
                                }
                            }
                        }
                    }
                }, callback);
        }

        private static void ParseWays(HashSet<long>? waysToLoad, Action<Way> action, IndexedTagFilters? tagFilters, byte[] readBuffer)
        {
            ReadOnlySpan<byte> dataSpan = readBuffer;
            Decompress(ref dataSpan, out var uncompressedBuffer, out var uncompressedSpan, out var uncompressedSize);
            var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
            var stringTableTargetLength = uncompressedSpan.Length - stringTableSize;
            var utf8ToIdMappings = new Dictionary<Memory<byte>, int>();
            var stringSpans = new List<Memory<byte>>();
            while (uncompressedSpan.Length > stringTableTargetLength)
            {
                var (index, size) = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan);
                if (tagFilters != null && tagFilters.StringLengths.TryGetValue(size, out var utf8StringList))
                {
                    foreach (var utf8String in utf8StringList)
                    {
                        if (uncompressedSpan.Slice(0, size).SequenceEqual(utf8String.Span))
                        {
                            utf8ToIdMappings.Add(utf8String, stringSpans.Count);
                        }
                    }
                }
                stringSpans.Add(new Memory<byte>(uncompressedBuffer, (int)uncompressedSize - uncompressedSpan.Length, size));
                uncompressedSpan = uncompressedSpan.Slice(size);
            }

            var stringIdFilters = new Dictionary<int, HashSet<int>?>();
            if (tagFilters != null)
            {
                foreach (var item in tagFilters.Utf8TagsFilter)
                {
                    if (item.TagValues.Count == 0)
                    {
                        stringIdFilters.Add(utf8ToIdMappings[item.TagKey], null);
                    }
                    else
                    {
                        var hashset = new HashSet<int>();
                        foreach (var tagValue in item.TagValues)
                        {
                            if (utf8ToIdMappings.TryGetValue(tagValue, out var val))
                                hashset.Add(val);
                        }
                        if (utf8ToIdMappings.TryGetValue(item.TagKey, out var val2))
                            stringIdFilters.Add(val2, hashset);
                    }
                }
            }

            var tagKeys = new List<int>();
            var tagValues = new List<int>();
            var nodes = new List<long>();
            var expectedValues = new Dictionary<int, HashSet<int>>();
            while (uncompressedSpan.Length > 0)
            {
                var primitiveGroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
                var expectedLengthAtEndOfPrimitiveGroup = uncompressedSpan.Length - primitiveGroupSize;
                while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitiveGroup)
                {
                    tagKeys.Clear();
                    tagValues.Clear();
                    expectedValues.Clear();

                    var (index, primitiveSize) = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan);
                    var expectedLengthAtEndOfPrimitive = uncompressedSpan.Length - primitiveSize;
                    if (index != 3)
                    {
                        throw new Exception();//Only expecting Ways here
                    }
                    BinSerialize.EnsureProtoIndexAndType(ref uncompressedSpan, 1, 0);
                    var wayId = BinSerialize.ReadPackedLong(ref uncompressedSpan);
                    bool accepted = false;
                    if (waysToLoad != null)
                    {
                        if (waysToLoad.Contains(wayId))
                        {
                            accepted = true;
                        }
                        else
                        {
                            uncompressedSpan = uncompressedSpan.Slice(uncompressedSpan.Length - expectedLengthAtEndOfPrimitive);
                            continue;
                        }
                    }
                    else
                    {
                        accepted = tagFilters == null || tagFilters.Utf8TagsFilter.Count == 0;
                    }
                    nodes.Clear();
                    while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitive)
                    {
                        var (innerIndex, type) = BinSerialize.ReadProtoIndexAndType(ref uncompressedSpan);
                        switch (innerIndex)
                        {
                            case 2://Tag keys
                                var sizeOfKeys = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                var expectedLengthAtEndOfKeys = uncompressedSpan.Length - sizeOfKeys;
                                while (uncompressedSpan.Length > expectedLengthAtEndOfKeys)
                                {
                                    int currentTagKeyId = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                    tagKeys.Add(currentTagKeyId);
                                    if (!accepted && stringIdFilters.TryGetValue(currentTagKeyId, out var hashset))
                                    {
                                        if (hashset == null)
                                        {
                                            accepted = true;
                                        }
                                        else
                                        {
                                            expectedValues.Add(tagKeys.Count, hashset);
                                        }
                                    }
                                }
                                // accepted would be true if we had matching key without value filter
                                // expectedValues would not be null if we had matching key, with value filters
                                // if none of this is true, there is no chance to have a match...
                                if (!accepted && expectedValues.Count == 0)
                                {
                                    // Skip to next relation
                                    uncompressedSpan = uncompressedSpan.Slice(uncompressedSpan.Length - expectedLengthAtEndOfPrimitive);
                                    continue;
                                }
                                break;
                            case 3://Tag values
                                var sizeOfValues = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                var expectedLengthAtEndOfValues = uncompressedSpan.Length - sizeOfValues;
                                while (uncompressedSpan.Length > expectedLengthAtEndOfValues)
                                {
                                    int currentTagValueId = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                    tagValues.Add(currentTagValueId);
                                    if (!accepted && expectedValues.TryGetValue(tagValues.Count, out var acceptableValues))
                                    {
                                        if (acceptableValues.Contains(currentTagValueId))
                                        {
                                            accepted = true;
                                        }
                                    }
                                }
                                if (!accepted)
                                {
                                    // Skip to next relation
                                    uncompressedSpan = uncompressedSpan.Slice(uncompressedSpan.Length - expectedLengthAtEndOfPrimitive);
                                    continue;
                                }
                                break;
                            case 4://Info
                                var sizeOfInfo = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                uncompressedSpan = uncompressedSpan.Slice(sizeOfInfo);
                                break;
                            case 8://node ids
                                var sizeOfRefs = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                var expectedLengthAtEndOfRefs = uncompressedSpan.Length - sizeOfRefs;
                                long nodeId = 0;
                                while (uncompressedSpan.Length > expectedLengthAtEndOfRefs)
                                {
                                    nodeId += BinSerialize.ReadZigZagLong(ref uncompressedSpan);
                                    nodes.Add(nodeId);
                                }
                                break;
                        }
                    }
                    if (accepted)
                    {
                        var tags = new TagsCollection(tagKeys.Count);
                        for (int i = 0; i < tagKeys.Count; i++)
                        {
                            tags.Add(new Tag(Encoding.UTF8.GetString(stringSpans[tagKeys[i]].Span), Encoding.UTF8.GetString(stringSpans[tagValues[i]].Span)));
                        }
                        action(new Way(wayId, nodes.ToArray(), tags));
                        waysToLoad?.Remove(wayId);
                        if (waysToLoad != null && waysToLoad.Count == 0)
                        {
                            ArrayPool<byte>.Shared.Return(uncompressedBuffer);
                            return;
                        }
                    }
                }
            }
            ArrayPool<byte>.Shared.Return(uncompressedBuffer);
        }

        public interface IWayNodesCallback
        {
            void Invoke(long nodeId, long wayId);
        }

        public static void DumpNodeToWaysMapping(PbfIndex index, string folder, HashSet<long> waysToLoad)
        {
            var fileOffsets = index.CalculateFileOffsets(waysToLoad, OsmGeoType.Way);
            const int NodesPerFile = 100_000_000;
            var groupsOfNodes = (int)(index.GetFirstNodeIdInLastOffset() / NodesPerFile + 1);
            if (Directory.Exists(folder))
                Directory.Delete(folder, true);
            Directory.CreateDirectory(folder);

            var executors = new List<NodeToWaysExecutor>();
            ParseWaysNodes(index, fileOffsets, () => {
                var executor = new NodeToWaysExecutor(folder, groupsOfNodes, NodesPerFile);
                executors.Add(executor);
                return executor;
            });
            executors.ToList().ForEach(f => f.Dispose());
        }

        class NodeToWaysExecutor : IWayNodesCallback, IDisposable
        {
            private int nodesPerFile;
            private readonly FileStream[] fileStreams;

            public NodeToWaysExecutor(string folder, int groupsOfNodes, int nodesPerFile)
            {
                this.nodesPerFile = nodesPerFile;
                fileStreams = Enumerable.Range(0, groupsOfNodes).Select((i) => {
                    var fileName = Path.Combine(folder, $"{i}-{Guid.NewGuid()}.bin");
                    return new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 1024 * 1024);
                }).ToArray();
            }

            public void Invoke(long wayId, long nodeId)
            {
                Span<byte> buffer = stackalloc byte[16];
                var fileStream = fileStreams[(int)(nodeId / nodesPerFile)];
                BinaryPrimitives.WriteInt64LittleEndian(buffer, nodeId);
                BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(8), wayId);
                fileStream.Write(buffer);
            }

            public void Dispose()
            {
                foreach (var stream in fileStreams)
                {
                    stream.Close();
                }
            }
        }
    }
}
