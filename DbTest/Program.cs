using OsmNightWatch.PbfParsing;
using System.Buffers.Binary;
using System.Diagnostics;

var path = @"C:\COSMOS\planet-230403.osm.pbf";
var sw = Stopwatch.StartNew();
var index = PbfIndexBuilder.BuildIndex(path);
var folder = "NodesToWaysGroups";

WaysParser.DumpNodeToWaysMapping(index, folder, new HashSet<long>() {
    898238622
});
Console.WriteLine(sw.Elapsed);

return;


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