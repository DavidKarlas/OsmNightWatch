using LibDeflate;
using System.Buffers.Binary;
using System.Buffers;
using System.Collections.Concurrent;
using System;
using System.Text;
using OsmSharp;
using OsmSharp.Tags;
using OsmSharp.IO.PBF;

namespace OsmNightWatch
{
    internal class PbfParser
    {
        class IndexedTagFilters
        {
            public readonly List<(Memory<byte> TagKey, List<Memory<byte>> TagValues)> Utf8RelationsTagsFilter;
            public Dictionary<int, List<Memory<byte>>> StringLengths = new();

            public IndexedTagFilters(List<(string TagKey, string TagValue)> tagFilters)
            {
                Utf8RelationsTagsFilter = new();
                foreach (var filterGroup in tagFilters.GroupBy(tf => tf.TagKey))
                {
                    Utf8RelationsTagsFilter.Add(
                        (
                            Encoding.UTF8.GetBytes(filterGroup.Key),
                            filterGroup.Where(g => !string.IsNullOrEmpty(g.TagValue))
                                .Select(g => (Memory<byte>)Encoding.UTF8.GetBytes(g.TagValue)).ToList())
                        );
                }
                foreach (var filter in Utf8RelationsTagsFilter)
                {
                    InsertNewString(filter.TagKey);
                    foreach (var tagValue in filter.TagValues)
                    {
                        InsertNewString(tagValue);
                    }
                }

                void InsertNewString(Memory<byte> utf8String)
                {
                    if (!StringLengths.TryGetValue(utf8String.Length, out var list))
                    {
                        StringLengths[utf8String.Length] = list = new List<Memory<byte>>();
                    }
                    list.Add(utf8String);
                }
            }
        }

        public static Dictionary<long, OsmSharp.Node> LoadNodes(string path, HashSet<long> nodesToLoad, PbfIndex index)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            var fileOffsets = index.CaclulateFileOffsets(nodesToLoad, OsmGeoType.Node);
            var nodeBags = new ConcurrentBag<OsmSharp.Node>();
            var tasks = new List<Task>();
#if DEBUG
            var slimSemaphore = new SemaphoreSlim(24);
#else
            var slimSemaphore = new SemaphoreSlim(24);
#endif
            var firstRelationPos = index.GetFirstRelationOffset();
            foreach (var fileOffset in fileOffsets)
            {
                file.Seek(fileOffset.FileOffset, SeekOrigin.Begin);
                var readBuffer = ArrayPool<byte>.Shared.Rent(16 * 1024 * 1024);
                var blobSize = ParseHeader(readBuffer, file, "OSMData");
                if (blobSize == 0)
                    break;
                if (firstRelationPos < file.Position)
                    break;
                if (file.Read(readBuffer, 0, blobSize) != blobSize)
                    throw new Exception("Too small file.");
                slimSemaphore.Wait();
                tasks.Add(Task.Run(() =>
                {
                    ParseNodes(nodeBags, fileOffset.AllElementsInside, readBuffer);
                    ArrayPool<byte>.Shared.Return(readBuffer);
                    slimSemaphore.Release();
                }));
                tasks.RemoveAll(task => task.IsCompleted);
            }
            Task.WhenAll(tasks).Wait();

            return nodeBags.ToDictionary(n => (long)n.Id!, n => n);
        }

        private static void ParseNodes(ConcurrentBag<OsmSharp.Node> nodeBags, HashSet<long> nodesToLoad, byte[] readBuffer)
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
                                if (nodesToLoad.Contains(nodes[i]))
                                {
                                    nodeBags.Add(new OsmSharp.Node()
                                    {
                                        Id = nodes[i],
                                        Latitude = DecodeLatLon(lats[i], lat_offset, granularity),
                                        Longitude = DecodeLatLon(lons[i], lon_offset, granularity)
                                    });
                                    nodesToLoad.Remove(nodes[i]);
                                    if (nodesToLoad.Count == 0)
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

        public static double DecodeLatLon(long valueOffset, long offset, long granularity)
        {
            return .000000001 * (offset + (granularity * valueOffset));
        }

        public static Dictionary<long, OsmSharp.Way> LoadWays(string path, HashSet<long> waysToLoad, PbfIndex index)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            var fileOffsets = index.CaclulateFileOffsets(waysToLoad, OsmGeoType.Way);
            var waysBag = new ConcurrentBag<OsmSharp.Way>();
            var tasks = new List<Task>();
#if DEBUG
            var slimSemaphore = new SemaphoreSlim(24);
#else
            var slimSemaphore = new SemaphoreSlim(24);
#endif
            var firstRelationPos = index.GetFirstRelationOffset();
            foreach (var fileOffset in fileOffsets)
            {
                file.Seek(fileOffset.FileOffset, SeekOrigin.Begin);
                var readBuffer = ArrayPool<byte>.Shared.Rent(16 * 1024 * 1024);
                var blobSize = ParseHeader(readBuffer, file, "OSMData");
                if (blobSize == 0)
                    break;
                if (firstRelationPos < file.Position)
                    break;
                if (file.Read(readBuffer, 0, blobSize) != blobSize)
                    throw new Exception("Too small file.");
                slimSemaphore.Wait();
                tasks.Add(Task.Run(() =>
                {
                    ParseWays(waysBag, fileOffset.AllElementsInside, readBuffer);
                    ArrayPool<byte>.Shared.Return(readBuffer);
                    slimSemaphore.Release();
                }));
                tasks.RemoveAll(task => task.IsCompleted);
            }
            Task.WhenAll(tasks).Wait();

            return waysBag.ToDictionary(w => (long)w.Id!, w => w);
        }

        private static void ParseWays(ConcurrentBag<OsmSharp.Way> waysBag, HashSet<long> waysToLoad, byte[] readBuffer)
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
                    if (!waysToLoad.Contains(wayId))
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
                    waysBag.Add(new OsmSharp.Way()
                    {
                        Id = wayId,
                        Nodes = nodes.ToArray()
                    });
                    waysToLoad.Remove(wayId);
                    if (waysToLoad.Count == 0)
                    {
                        ArrayPool<byte>.Shared.Return(uncompressbuffer);
                        return;
                    }
                }
            }
            throw new InvalidOperationException("if (waysToLoad.Count == 0) should get us out of here before we get here...");
        }

        public static List<OsmSharp.Relation> Parse(string path, List<(string TagKey, string TagValue)> relationsTagsFilter, PbfIndex index, bool ignoreCache = false)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            file.Seek(index.GetFirstRelationOffset(), SeekOrigin.Begin);

            var relationsBag = new ConcurrentBag<OsmSharp.Relation>();
            var tasks = new List<Task>();
#if DEBUG
            var slimSemaphore = new SemaphoreSlim(24);
#else
            var slimSemaphore = new SemaphoreSlim(24);
#endif
            var indexedFilters = new IndexedTagFilters(relationsTagsFilter);

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
                tasks.Add(Task.Run(() =>
                {
                    ParseRelations(relationsBag, indexedFilters, readBuffer);
                    ArrayPool<byte>.Shared.Return(readBuffer);
                    slimSemaphore.Release();
                }));
                tasks.RemoveAll(task => task.IsCompleted);
            }
            Task.WhenAll(tasks).Wait();

            return relationsBag.ToList();
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

        static void ParseRelations(ConcurrentBag<OsmSharp.Relation> relationsBag, IndexedTagFilters tagFilters, byte[] readBuffer)
        {
            ReadOnlySpan<byte> dataSpan = readBuffer;
            Decompress(ref dataSpan, out var uncompressbuffer, out var uncompressedSpan, out var uncompressedSize);
            var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
            var stringTableTargetLength = uncompressedSpan.Length - stringTableSize;
            var utf8ToIdMappings = new Dictionary<Memory<byte>, int>();
            var stringSpans = new List<Memory<byte>>();
            while (uncompressedSpan.Length > stringTableTargetLength)
            {
                var (index, size) = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan);
                if (tagFilters.StringLengths.TryGetValue(size, out var utf8StringList))
                {
                    foreach (var utf8String in utf8StringList)
                    {
                        if (uncompressedSpan.Slice(0, size).SequenceEqual(utf8String.Span))
                        {
                            utf8ToIdMappings.Add(utf8String, stringSpans.Count);
                        }
                    }
                }
                stringSpans.Add(new Memory<byte>(uncompressbuffer, (int)uncompressedSize - uncompressedSpan.Length, size));
                uncompressedSpan = uncompressedSpan.Slice(size);
            }

            var idFilters = new Dictionary<int, HashSet<int>?>();
            foreach (var item in tagFilters.Utf8RelationsTagsFilter)
            {
                if (item.TagValues.Count == 0)
                {
                    idFilters.Add(utf8ToIdMappings[item.TagKey], null);
                }
                else
                {
                    var hashset = new HashSet<int>();
                    foreach (var tagValue in item.TagValues)
                    {
                        hashset.Add(utf8ToIdMappings[tagValue]);
                    }
                    idFilters.Add(utf8ToIdMappings[item.TagKey], hashset);
                }
            }

            var tagKeys = new List<int>();
            var tagValues = new List<int>();
            var roles = new List<int>();
            var membersIds = new List<long>();
            var memberTypes = new List<OsmSharp.OsmGeoType>();
            var expectedValues = new Dictionary<int, HashSet<int>>();
            while (uncompressedSpan.Length > 0)
            {
                var primitivegroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan).size;
                var expectedLengthAtEndOfPrimitiveGroup = uncompressedSpan.Length - primitivegroupSize;
                while (uncompressedSpan.Length > expectedLengthAtEndOfPrimitiveGroup)
                {
                    tagKeys.Clear();
                    tagValues.Clear();
                    roles.Clear();
                    membersIds.Clear();
                    memberTypes.Clear();
                    expectedValues.Clear();

                    var (index, primitiveSize) = BinSerialize.ReadProtoByteArraySize(ref uncompressedSpan);
                    var expectedLengthAtEndOfPrimitive = uncompressedSpan.Length - primitiveSize;
                    if (index != 4)
                    {
                        throw new Exception();//Only expecting Relations here
                    }
                    BinSerialize.EnsureProtoIndexAndType(ref uncompressedSpan, 1, 0);
                    var relationId = BinSerialize.ReadPackedLong(ref uncompressedSpan);

                    bool accepted = false;
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
                                    if (!accepted && idFilters.TryGetValue(currentTagKeyId, out var hashset))
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
                            case 8://roles_sid
                                var sizeOfRoles = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                var expectedLengthAtEndOfRoles = uncompressedSpan.Length - sizeOfRoles;
                                while (uncompressedSpan.Length > expectedLengthAtEndOfRoles)
                                {
                                    int role_sid = BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                    roles.Add(role_sid);
                                }
                                break;
                            case 9://memids
                                var sizeOfMemids = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                var expectedLengthAtEndOfMemids = uncompressedSpan.Length - sizeOfMemids;
                                long memId = 0;
                                while (uncompressedSpan.Length > expectedLengthAtEndOfMemids)
                                {
                                    memId += BinSerialize.ReadZigZagLong(ref uncompressedSpan);
                                    membersIds.Add(memId);
                                }
                                break;
                            case 10://types
                                var sizeOfTypes = BinSerialize.ReadPackedUnsignedInteger(ref uncompressedSpan);
                                var expectedLengthAtEndOfTypes = uncompressedSpan.Length - sizeOfTypes;
                                while (uncompressedSpan.Length > expectedLengthAtEndOfTypes)
                                {
                                    var memberType = (OsmSharp.OsmGeoType)BinSerialize.ReadPackedInt(ref uncompressedSpan);
                                    memberTypes.Add(memberType);
                                }
                                break;
                            default:
                                throw new Exception();
                        }
                    }
                    if (accepted)
                    {
                        var members = new RelationMember[membersIds.Count];
                        for (int i = 0; i < members.Length; i++)
                        {
                            members[i] = new RelationMember(membersIds[i], Encoding.UTF8.GetString(stringSpans[roles[i]].Span), memberTypes[i]);
                        }
                        var tags = new TagsCollection(tagKeys.Count);
                        for (int i = 0; i < tagKeys.Count; i++)
                        {
                            tags.Add(new Tag(Encoding.UTF8.GetString(stringSpans[tagKeys[i]].Span), Encoding.UTF8.GetString(stringSpans[tagValues[i]].Span)));
                        }
                        relationsBag.Add(new OsmSharp.Relation()
                        {
                            Id = (long)relationId,
                            Members = members,
                            Tags = tags
                        });
                    }
                }
            }
            ArrayPool<byte>.Shared.Return(uncompressbuffer);
        }

        private static void Decompress(ref ReadOnlySpan<byte> readDataR, out byte[] uncompressbuffer, out ReadOnlySpan<byte> uncompressedData, out uint uncompressedDataSize)
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
