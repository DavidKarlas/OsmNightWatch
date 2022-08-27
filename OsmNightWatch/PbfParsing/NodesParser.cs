using OsmSharp;
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

        public static Dictionary<long, Node> LoadNodes(string path, HashSet<long> nodesToLoad, PbfIndex index)
        {
            var fileOffsets = index.CaclulateFileOffsets(nodesToLoad, OsmGeoType.Node);
            var nodeBags = new ConcurrentBag<Node>();
            ParallelParse(path, fileOffsets, (HashSet<long>? relevantIds, byte[] readBuffer) =>
            {
                ParseNodes(nodeBags, relevantIds, readBuffer);
            });

            return nodeBags.ToDictionary(n => (long)n.Id!, n => n);
        }

        private static void ParseNodes(ConcurrentBag<Node> nodeBags, HashSet<long>? nodesToLoad, byte[] readBuffer)
        {
            ReadOnlySpan<byte> dataSpan = readBuffer;
            Decompress(ref dataSpan, out var uncompressbuffer, out var uncompressedSpan, out var uncompressedSize);
            var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
            uncompressedSpan = uncompressedSpan.Slice(stringTableSize);

            var nodes = new List<long>();
            var lats = new List<long>();
            var lons = new List<long>();
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
                        var primitivegroupSize = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                        var expectedLengthAtEndOfPrimitiveGroup = uncompressedSpan.Length - primitivegroupSize;
                        nodes.Clear();
                        lats.Clear();
                        lons.Clear();
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
                                        uncompressedSpan = uncompressedSpan.Slice(sizeOfKeysVals);
                                        break;
                                }
                            }
                            for (int i = 0; i < nodes.Count; i++)
                            {
                                if (nodesToLoad?.Contains(nodes[i]) ?? true)
                                {
                                    nodeBags.Add(new Node()
                                    {
                                        Id = nodes[i],
                                        Latitude = DecodeLatLon(lats[i], lat_offset, granularity),
                                        Longitude = DecodeLatLon(lons[i], lon_offset, granularity)
                                    });
                                    nodesToLoad?.Remove(nodes[i]);
                                    if (nodesToLoad != null && nodesToLoad.Count == 0)
                                    {
                                        ArrayPool<byte>.Shared.Return(uncompressbuffer);
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
            throw new InvalidOperationException("if (waysToLoad.Count == 0) should get us out of here before we get here...");
        }
    }
}