using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.BrokenCoastline;
using OsmNightWatch.Analyzers.OpenPolygon;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using OsmSharp.Replication;

var path = @"C:\COSMOS\planet-220829.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var analyzers = new IOsmAnalyzer[] { /*new AdminOpenPolygonAnalyzer(),new BrokenCoastlineAnalyzer()*/  };
var keyValueDatabase = new KeyValueDatabase(Path.GetFullPath("KeyValueData"));
var dbWithChagnes = new OsmDatabaseWithReplicationData(pbfDb, keyValueDatabase);

if (keyValueDatabase.GetSequenceNumber() is not long nextSequenceId)
{
    nextSequenceId = await Utils.GetSequenceNumberFromPbf(index);
}

while (true)
{
    var changeset = await ReplicationConfig.Minutely.DownloadDiff(nextSequenceId);
    if (changeset is null)
    {
        await Task.Delay(TimeSpan.FromMinutes(1));
        continue;
    }
    var replicationState = await ReplicationConfig.Minutely.GetReplicationState(nextSequenceId);
    if (replicationState is null)
    {
        throw new InvalidOperationException("How we got changeset but no replication state?");
    }
    using var tx = keyValueDatabase.BeginTransaction();

    var data = new IssuesData()
    {
        DateTime = replicationState.StartTimestamp,
        AllIssues = new List<IssueData>()
    };
    dbWithChagnes.ApplyChangeset(changeset, tx);
    foreach (var analyzer in analyzers)
    {
        Console.WriteLine($"{DateTime.Now} Starting {analyzer.AnalyzerName}.");
        var relevatThings = dbWithChagnes.Filter(analyzer.GetFilters()).ToArray();
        Console.WriteLine($"{DateTime.Now} Filtered relevant things {relevatThings.Length}.");
        var issues = analyzer.Initialize(relevatThings, dbWithChagnes, dbWithChagnes).ToList();
        Console.WriteLine($"{DateTime.Now} Found {issues.Count} issues.");
        data.AllIssues.AddRange(issues);
    }

    nextSequenceId++;
    keyValueDatabase.SetSequenceNumber(nextSequenceId, tx);
    var code = tx.Commit();
    IssuesUploader.Upload(data);
}

