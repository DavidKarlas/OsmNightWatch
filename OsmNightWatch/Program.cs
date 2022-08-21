using OsmNightWatch;
using System.Buffers;
using System.Diagnostics;
using System.Globalization;

var sw = Stopwatch.StartNew();
//var path = @"C:\COSMOS\planet-220606.osm.pbf";
var path = @"C:\COSMOS\planet-220815.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);

var nodesToLoad = new HashSet<long>();

var relevantRelations = new HashSet<OsmSharp.Relation>(PbfParser.Parse(path, new List<(string TagKey, string TagValue)>()
{
    ("boundary","administrative" )
}, index));

var waysToLoad = new HashSet<long>();
//TODO: Recursivly load all Relations members
foreach (var relation in relevantRelations.ToArray())
{
    foreach (var item in relation.Members)
    {
        if (item.Role == "outer" || item.Role == "inner")
        {
            switch (item.Type)
            {
                case OsmSharp.OsmGeoType.Way:
                    waysToLoad.Add(item.Id);
                    break;
                case OsmSharp.OsmGeoType.Relation:
                    relevantRelations.Remove(relation);//TODO: undo this HACK...
                    break;
                case OsmSharp.OsmGeoType.Node:
                    throw new Exception("Fix it now on osm.org!");
            }
        }
    }
}

var relevantWays = PbfParser.LoadWays(path, waysToLoad, index);
var sw3 = Stopwatch.StartNew();
foreach (var relation in relevantRelations)
{
    if(relation.Tags.TryGetValue("admin_level", out var lvl))
    {
        if (double.Parse(lvl, CultureInfo.InvariantCulture) > 7)
            continue;
    }
    else
    {
        continue;
    }
    var sw2 = Stopwatch.StartNew();
    if (new RelationValidationTest().Visit(relation, relevantWays))
    {
        Console.WriteLine("Fail:" + relation.Id + " " + sw2.ElapsedMilliseconds);
    }
    //if (sw2.ElapsedMilliseconds > 100)
    //{
    //    Console.WriteLine("Time:" + relation.Id + " " + sw2.ElapsedMilliseconds);
    //}
}
Console.WriteLine("Total:" + sw3.Elapsed);
//TODO: Load nodes and build full geometry
//foreach (var way in relevantWays.Values)
//{
//    nodesToLoad.UnionWith(way.Nodes);
//}
//var relevantNodes = PbfParser.LoadNodes(path, nodesToLoad, index);
//var db = new OsmDatabase(relevantRelations.ToDictionary(a => (long)a.Id!, a => a), relevantWays, relevantNodes);
//foreach (var rel in relevantRelations)
//{
//    var completeRel = rel.CreateComplete(db);
//    var geo = DefaultFeatureInterpreter.DefaultInterpreter.Interpret(completeRel);
//}

sw.Stop();

Console.WriteLine(sw.Elapsed);
Console.WriteLine(relevantRelations.Count);
Console.WriteLine(relevantWays.Count);




//var uncompressedDataSize = BinSerialize.ReadProtoUInt32(ref osmHeaderBuffer);
//var compressedDataSize = BinSerialize.ReadProtoByteArraySize(ref osmHeaderBuffer);
//var array = ArrayPool<byte>.Shared.Rent((int)uncompressedDataSize);

////var exInput = new MemoryStream(readerBuffer.Slice(0, (int)compressedDataSize).ToArray());
////exInput.Position = 0;
////var zlibStream = new ZLibStream(exInput, CompressionMode.Decompress);
////var ms = new MemoryStream(array);
////zlibStream.CopyTo(ms);
//ZLibHelper.Unzip(osmHeaderBuffer.Slice(0, (int)compressedDataSize), array, uncompressedDataSize);

//var uncompressedData = new ReadOnlySpan<byte>(array, 0, (int)uncompressedDataSize);
//var stringTableSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
//var targetLength = uncompressedData.Length - stringTableSize;
//while (uncompressedData.Length > targetLength)
//{
//    var size = BinSerialize.ReadProtoString(ref uncompressedData);
//}
//var primitivegroupSize = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
//var primitivegroupTargetLength = uncompressedData.Length - primitivegroupSize;

//var denseNodes = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
//var sizeOfIds = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
//targetLength = uncompressedData.Length - sizeOfIds;
//long currentId = 0;
//while (uncompressedData.Length > targetLength)
//{
//    currentId += BinSerialize.ReadPackedInteger(ref uncompressedData);
//}
//var denseNodes3 = BinSerialize.ReadPackedInteger(ref uncompressedData);
//var denseNodes4 = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
//var nodeVersion= BinSerialize.ReadProtoUInt32(ref uncompressedData);


//var bboxArraySize = BinSerialize.ReadProtoByteArraySize(ref uncompressedData);
//uncompressedData = uncompressedData.Slice(bboxArraySize);
//var features = BinSerialize.ReadProtoString(ref uncompressedData);
//var features2 = BinSerialize.ReadProtoString(ref uncompressedData);
//var features3 = BinSerialize.ReadProtoString(ref uncompressedData);
//var features4 = BinSerialize.ReadProtoString(ref uncompressedData);
//while (true)
//{
//}