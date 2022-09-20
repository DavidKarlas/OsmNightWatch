using OsmSharp;
using System.Buffers;
using System.Collections.Concurrent;
using static OsmNightWatch.PbfParsing.ParsingHelper;

namespace OsmNightWatch.PbfParsing
{
    public static class WaysParser
    {
        public static Dictionary<long, Way> LoadWays(HashSet<long> waysToLoad, PbfIndex index)
        {
            var fileOffsets = index.CaclulateFileOffsets(waysToLoad, OsmGeoType.Way);
            var waysBag = new ConcurrentBag<Way>();
            ParallelParse(index.PbfPath, fileOffsets, (HashSet<long>? relevantIds, byte[] readBuffer) =>
            {
                ParseWays(waysBag, relevantIds, readBuffer);
            });

            return waysBag.ToDictionary(w => (long)w.Id!, w => w);
        }

        private static void ParseWays(ConcurrentBag<Way> waysBag, HashSet<long>? waysToLoad, byte[] readBuffer)
        {
            ReadOnlySpan<byte> dataSpan = readBuffer;
            Decompress(ref dataSpan, out var uncompressbuffer, out var uncompressedSpan, out var uncompressedSize);
            var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
            uncompressedSpan = uncompressedSpan.Slice(stringTableSize);

            var nodes = new List<long>();
            while (uncompressedSpan.Length > 0)
            {
                var primitivegroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
                var expectedLengthAtEndOfPrimitiveGroup = uncompressedSpan.Length - primitivegroupSize;
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
                    if (!(waysToLoad?.Contains(wayId) ?? true))
                    {
                        uncompressedSpan = uncompressedSpan.Slice(uncompressedSpan.Length - expectedLengthAtEndOfPrimitive);
                        continue;
                    }
                    nodes.Clear();
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
                                    nodes.Add(nodeId);
                                }
                                break;
                        }
                    }
                    waysBag.Add(new Way()
                    {
                        Id = wayId,
                        Nodes = nodes.ToArray()
                    });
                    waysToLoad?.Remove(wayId);
                    if (waysToLoad != null && waysToLoad.Count == 0)
                    {
                        ArrayPool<byte>.Shared.Return(uncompressbuffer);
                        return;
                    }
                }
            }
            throw new InvalidOperationException("if (waysToLoad.Count == 0) should get us out of here before we get here...");
        }
    }
}
