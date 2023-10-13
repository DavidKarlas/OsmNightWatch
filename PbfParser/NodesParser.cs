using OsmSharp;
using OsmSharp.IO.PBF;
using OsmSharp.Tags;
using System.Buffers;
using System.Collections.Concurrent;
using static OsmNightWatch.PbfParsing.ParsingHelper;

namespace OsmNightWatch.PbfParsing
{
    public static class NodesParser
    {
        private static double DecodeLatLon(long valueOffset, long offset, long granularity)
        {
            return .000000001 * (offset + granularity * valueOffset);
        }

        public static IReadOnlyCollection<Node> LoadNodes(HashSet<long>? nodesToLoad, PbfIndex index)
        {
            if (nodesToLoad == null || nodesToLoad.Count == 0)
                return Array.Empty<Node>();
            var fileOffsets = index.CalculateFileOffsets(nodesToLoad, OsmGeoType.Node);
            var nodesBag = new ConcurrentBag<Node>();
            ParallelParse(index.PbfPath, fileOffsets, (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                ParseNodes(relevantIds, (node) => {
                    nodesBag.Add(node);
                }, null, readBuffer);
            });

            return nodesBag;
        }

        public static IEnumerable<Node> Parse(IEnumerable<ElementFilter> filters, PbfIndex index)
        {
            var indexFilters = new IndexedTagFilters(filters.Where(f => f.GeoType == OsmGeoType.Node).SelectMany(f => f.Tags));
            var nodesQueue = new ConcurrentQueue<List<Node>>();
            var task = Task.Run(() => ParallelParse(index.PbfPath, index.GetAllNodeFileOffsets().Select(o => (o, (HashSet<long>?)null)).ToList(),
                (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                    var nodeBag = new List<Node>();
                    ParseNodes(null, (node) => {
                        nodeBag.Add(node);
                    }, indexFilters, readBuffer);
                    nodesQueue.Enqueue(nodeBag);
                }));
            while (task.IsCompleted == false || nodesQueue.Count > 0)
            {
                while (nodesQueue.TryDequeue(out var nodesBag))
                {
                    foreach (var node in nodesBag)
                    {
                        yield return node;
                    }
                }
            }
        }

        public static void Process(Action<Node> action, IEnumerable<ElementFilter> filters, PbfIndex index)
        {
            var indexFilters = new IndexedTagFilters(filters.Where(f => f.GeoType == OsmGeoType.Node).SelectMany(f => f.Tags));
            ParallelParse(index.PbfPath, index.GetAllNodeFileOffsets().Select(o => (o, (HashSet<long>?)null)).ToList(),
                (HashSet<long>? relevantIds, byte[] readBuffer, object? state) => {
                    ParseNodes(null, action, indexFilters, readBuffer);
                });
        }

        private static void ParseNodes(HashSet<long>? nodesToLoad, Action<Node> action, IndexedTagFilters? tagFilters, byte[] readBuffer)
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
                        if (utf8ToIdMappings.TryGetValue(item.TagKey, out var val))
                            stringIdFilters.Add(val, null);
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

            if (tagFilters?.Utf8TagsFilter.Count > 0 && stringIdFilters.Count == 0)
            {
                ArrayPool<byte>.Shared.Return(uncompressedBuffer);
                return;
            }


            var nodes = new List<long>();
            var lats = new List<long>();
            var lons = new List<long>();
            var keyVals = new Dictionary<int, List<(int key, int val)>>();
            int granularity = 100;
            int date_granularity = 100;
            long lat_offset = 0;
            long lon_offset = 0;
            while (uncompressedSpan.Length > 0)
            {
                var (blockIndex, blockType) = BinSerialize.ReadProtoIndexAndType(ref uncompressedSpan);
                switch (blockIndex)
                {
                    case 2:
                        var filteredNodesIndexes = new HashSet<int>();
                        var primitivegroupSize = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                        var expectedLengthAtEndOfPrimitiveGroup = uncompressedSpan.Length - primitivegroupSize;
                        nodes.Clear();
                        lats.Clear();
                        lons.Clear();
                        keyVals.Clear();
                        while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitiveGroup)
                        {
                            var (index, primitiveSize) = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan);
                            var expectedLengthAtEndOfPrimitive = uncompressedSpan.Length - primitiveSize;
                            if (index != 2)
                            {
                                throw new Exception();//Only expecting DenseNodes here
                            }
                            while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitive)
                            {
                                var (innerIndex, type) = BinSerialize.ReadProtoIndexAndType(ref uncompressedSpan);
                                switch (innerIndex)
                                {
                                    case 1://Ids
                                        var sizeOfIds = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        var expectedLengthAtEndOfIds = uncompressedSpan.Length - sizeOfIds;
                                        long currentNodeId = 0;
                                        while (uncompressedSpan.Length > expectedLengthAtEndOfIds)
                                        {
                                            currentNodeId += BinSerialize.ReadZigZagLong(ref uncompressedSpan);
                                            nodes.Add(currentNodeId);
                                        }
                                        break;
                                    case 5://Info
                                        var sizeOfInfo = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        uncompressedSpan = uncompressedSpan.Slice(sizeOfInfo);
                                        break;
                                    case 8://Lat values
                                        var sizeOfLats = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        var expectedLengthAtEndOfLats = uncompressedSpan.Length - sizeOfLats;
                                        long currentLat = 0;
                                        while (uncompressedSpan.Length > expectedLengthAtEndOfLats)
                                        {
                                            currentLat += BinSerialize.ReadZigZagLong(ref uncompressedSpan);
                                            lats.Add(currentLat);
                                        }
                                        break;
                                    case 9://Lon values
                                        var sizeOfLons = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        var expectedLengthAtEndOfLons = uncompressedSpan.Length - sizeOfLons;
                                        long currentLon = 0;
                                        while (uncompressedSpan.Length > expectedLengthAtEndOfLons)
                                        {
                                            currentLon += BinSerialize.ReadZigZagLong(ref uncompressedSpan);
                                            lons.Add(currentLon);
                                        }
                                        break;
                                    case 10://keys_vals 
                                        var sizeOfKeysVals = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                        var expectedLengthAtEndOfKeysVals = uncompressedSpan.Length - sizeOfKeysVals;
                                        int nodeIndex = 0;
                                        List<(int key, int val)>? current = null;
                                        while (uncompressedSpan.Length > expectedLengthAtEndOfKeysVals)
                                        {
                                            var key = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                            if (key == 0)
                                            {
                                                current = null;
                                                nodeIndex++;
                                                continue;
                                            }
                                            var val = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                            if (current == null)
                                            {
                                                keyVals[nodeIndex] = current = new List<(int key, int val)>();
                                            }
                                            current.Add(((int)key, (int)val));
                                            if (stringIdFilters.TryGetValue((int)key, out var hashset))
                                            {
                                                if (hashset == null)
                                                {
                                                    filteredNodesIndexes.Add(nodeIndex);
                                                }
                                                else
                                                {
                                                    if (hashset.Contains((int)val))
                                                    {
                                                        filteredNodesIndexes.Add(nodeIndex);
                                                    }
                                                }
                                            }
                                        }
                                        break;
                                }
                            }
                            for (int i = 0; i < nodes.Count; i++)
                            {
                                if (nodesToLoad?.Contains(nodes[i]) ?? true && filteredNodesIndexes.Contains(i))
                                {
                                    TagsCollection? tags = null;
                                    if (keyVals.TryGetValue(i, out var val))
                                    {
                                        tags = new OsmSharp.Tags.TagsCollection(keyVals[i].Count);
                                        for (int j = 0; j < keyVals[i].Count; j++)
                                        {
                                            tags.Add(new OsmSharp.Tags.Tag(System.Text.Encoding.UTF8.GetString(stringSpans[keyVals[i][j].key].Span), System.Text.Encoding.UTF8.GetString(stringSpans[keyVals[i][j].val].Span)));
                                        }
                                    }
                                    action(new Node(nodes[i], DecodeLatLon(lats[i], lat_offset, granularity), DecodeLatLon(lons[i], lon_offset, granularity), tags));
                                    nodesToLoad?.Remove(nodes[i]);
                                    if (nodesToLoad != null && nodesToLoad.Count == 0)
                                    {
                                        ArrayPool<byte>.Shared.Return(uncompressedBuffer);
                                        return;
                                    }
                                }
                            }
                        }
                        break;
                    case 17://granularity
                        granularity = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                        break;
                    case 18://date_granularity 
                        date_granularity = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                        break;
                    case 19://lat_offset
                        lat_offset = BinSerialize.ReadPackedLong(ref uncompressedSpan);
                        break;
                    case 20://lat_offset
                        lon_offset = BinSerialize.ReadPackedLong(ref uncompressedSpan);
                        break;
                }
            }
            ArrayPool<byte>.Shared.Return(uncompressedBuffer);
        }

        class NodeCollector : IPBFOsmPrimitiveConsumer
        {
            public List<OsmSharp.Node> Nodes { get; } = new();

            public void ProcessNode(PrimitiveBlock block, OsmSharp.IO.PBF.Node node)
            {
                Nodes.Add(Encoder.DecodeNode(block, node));
            }

            public void ProcessRelation(PrimitiveBlock block, OsmSharp.IO.PBF.Relation relation)
            {
                throw new NotImplementedException();
            }

            public void ProcessWay(PrimitiveBlock block, OsmSharp.IO.PBF.Way way)
            {
                throw new NotImplementedException();
            }
        }

        public static OsmSharp.Node[] LoadNodesWithMetadata(string pbfPath, long offset)
        {
            using var fileStream = new FileStream(pbfPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            fileStream.Position = offset;
            var osmReader = new PBFReader(fileStream);
            var block = osmReader.MoveNext();
            var collector = new NodeCollector();
            Encoder.Decode(block, collector, false, false, false, out _, out _, out _);
            return collector.Nodes.ToArray();
        }
    }
}