﻿using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.OpenPolygon;
using OsmNightWatch.PbfParsing;
using OsmSharp.Replication;

var path = @"C:\COSMOS\planet-220815.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var dbWithChagnes = new OsmDatabaseWithReplicationData(pbfDb);
var analyzers = new IOsmAnalyzer[] { new AdminOpenPolygonAnalyzer() };
var analzerHosts = analyzers.Select(analyzer => new AnalyzerHost(analyzer, dbWithChagnes)).ToArray();
var kvDatabase = new KeyValueDatabase(Path.GetFullPath("KeyValueData"));

if(kvDatabase.GetSequenceNumber() is not long sequenceNumber)
{
    sequenceNumber = await Utils.GetSequenceNumberFromPbf(index);
    
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
    using var tx = kvDatabase.BegingTransaction();



    sequenceNumber++;
    kvDatabase.SetSequenceNumber(sequenceNumber, tx);
    dbWithChagnes.ApplyChangeset(changeset, tx);
    tx.Commit();
}

