using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.OpenPolygon;
using OsmNightWatch.PbfParsing;
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;

var dummy = new OsmNightWatch.Lib.IssuesData();
dummy.DateTime = DateTime.Now;
dummy.AllIssues.Add(new OsmNightWatch.Lib.IssueData()
{
    IssueType = "OpenAdminPolygon",
    OsmType = "relation",
    OsmId = "1854615"
});
dummy.AllIssues.Add(new OsmNightWatch.Lib.IssueData()
{
    IssueType = "OpenAdminPolygon",
    OsmType = "relation",
    OsmId = "8795605"
});
dummy.AllIssues.Add(new OsmNightWatch.Lib.IssueData()
{
    IssueType = "OpenAdminPolygon",
    OsmType = "relation",
    OsmId = "6064117"
});

File.WriteAllText("issues.json", JsonSerializer.Serialize(dummy));

return;

HackOsmReplicationBug();

var sw = Stopwatch.StartNew();
var path = @"C:\COSMOS\planet-220815.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var pbfDatabaseWithProcessedChangesets = new OsmDatabaseWithReplicationData(pbfDb);
var analyzers = new IOsmAnalyzer[] { new AdminOpenPolygonAnalyzer() };

foreach (var analyzer in analyzers)
{
    var analzerHost = new AnalyzerHost(analyzer, pbfDatabaseWithProcessedChangesets);

}


//var replicationData = new ReplicationData(path, index, @"C:\COSMOS\replication\");
//await replicationData.InitializeAsync();

Console.WriteLine(sw.Elapsed);

static void HackOsmReplicationBug()
{
    AssemblyLoadContext.Default.Resolving += OnAssemblyResolve;

    Assembly? OnAssemblyResolve(AssemblyLoadContext arg1, AssemblyName arg2)
    {
        var assembly = Assembly.Load(new AssemblyName(arg2.Name));
        if (assembly != null)
        {
            return assembly;
        }
        return null;
    }
}