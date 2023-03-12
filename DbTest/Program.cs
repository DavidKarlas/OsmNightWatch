// See https://aka.ms/new-console-template for more information
using OsmNightWatch;
using OsmNightWatch.PbfParsing;
using System.Buffers.Binary;
using System.Diagnostics;
using Tenray.ZoneTree;

var path = @"C:\COSMOS\planet-230130.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
const int NodesPerFile = 100_000_000;
var billionsOfNodes = (int)(index.GetFirstNodeIdInLastOffset() / NodesPerFile + 1);
if (Directory.Exists("NodesToWaysGroups"))
    Directory.Delete("NodesToWaysGroups", true);
Directory.CreateDirectory("NodesToWaysGroups");
var fileStreams = Enumerable.Range(0, billionsOfNodes).Select((i) => {
    var fileName = $"NodesToWaysGroups\\{i}.bin";
    return new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
}).ToArray();
var sw = Stopwatch.StartNew();
long count = 0;
WaysParser.ParseWaysNodes(index, (wayId, nodeId) => {
    //Span<byte> buffer = stackalloc byte[16];
    //var fileStream = fileStreams[(int)(nodeId / NodesPerFile)];
    //BinaryPrimitives.WriteInt64LittleEndian(buffer, nodeId);
    //BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(8), wayId);
    ////lock (fileStream)
    //{
    //    fileStream.Write(buffer);
    //}
});
fileStreams.ToList().ForEach(f => f.Dispose());
Console.WriteLine(sw.Elapsed);

//var zoneTree = new ZoneTreeFactory<long, long[]>()
//    .SetDiskSegmentMaximumCachedBlockCount(1)
//    .SetValueSerializer(new LongArraySerializer())
//   .OpenOrCreate();
//long count = 0;
//foreach (var way in WaysParser.Parse(Array.Empty<ElementFilter>(), index))
//{
//    foreach (var node in way.Nodes)
//    {
//        if (zoneTree.TryGet(node, out var list))
//        {
//            list = list.Append((long)way.Id!).ToArray();
//        }
//        else
//        {
//            list = new[] { (long)way.Id! };
//        }
//        zoneTree.Upsert(node, list);
//    }
//    if (count++ % 1000000 == 0)
//    {
//        Console.WriteLine($"Processed {count} ways.");
//        zoneTree.Dispose();
//        zoneTree = new ZoneTreeFactory<long, long[]>()
//        .SetValueSerializer(new LongArraySerializer())
//       .OpenOrCreate();
//    }
//}

Console.WriteLine();