using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.OpenPolygon;
using OsmNightWatch.PbfParsing;
using OsmSharp.Replication;

var path = @"C:\Users\davkar\Downloads\australia-oceania-latest.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var dbWithChagnes = new OsmDatabaseWithReplicationData(pbfDb);
var analyzers = new IOsmAnalyzer[] { new AdminOpenPolygonAnalyzer() };
var keyValueDatabase = new KeyValueDatabase(Path.GetFullPath("KeyValueData"));

if (keyValueDatabase.GetSequenceNumber() is not long sequenceNumber)
{
    sequenceNumber = await Utils.GetSequenceNumberFromPbf(index);

    foreach (var analyzer in analyzers)
    {
        var relevatThings = pbfDb.Filter(analyzer.GetFilters());
        var issues = analyzer.Initialize(relevatThings, pbfDb, pbfDb);
    }
}

while (true)
{
    var changeset = await ReplicationConfig.Minutely.DownloadDiff(sequenceNumber);
    if (changeset is null)
    {
        await Task.Delay(TimeSpan.FromMinutes(1));
        continue;
    }
    var newDb = new OsmDatabaseWithChangeset(dbWithChagnes, changeset);
    using var tx = keyValueDatabase.BegingTransaction();

    foreach (var analyzer in analyzers)
    {
        var issues = analyzer.AnalyzeChanges(changeset, dbWithChagnes, newDb);
    }

    sequenceNumber++;
    keyValueDatabase.SetSequenceNumber(sequenceNumber, tx);
    dbWithChagnes.ApplyChangeset(changeset, tx);
    tx.Commit();
}

