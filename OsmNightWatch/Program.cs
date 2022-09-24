using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.BrokenCoastline;
using OsmNightWatch.Analyzers.OpenPolygon;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using OsmSharp.Changesets;
using OsmSharp.Replication;
using System.IO.Compression;
using System.Xml.Serialization;

HttpClient httpClient = new HttpClient();
ThreadLocal<XmlSerializer> ThreadLocalXmlSerializer = new ThreadLocal<XmlSerializer>(() => new XmlSerializer(typeof(OsmChange)));

var resolution = ReplicationConfig.Daily;
var path = @"C:\COSMOS\planet-220829.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var analyzers = new IOsmAnalyzer[] { /*new AdminOpenPolygonAnalyzer(),*/new BrokenCoastlineAnalyzer() };
var keyValueDatabase = new KeyValueDatabase(Path.GetFullPath("KeyValueData"));
var dbWithChagnes = new OsmDatabaseWithReplicationData(pbfDb, keyValueDatabase);
if (keyValueDatabase.GetSequenceNumber() is not long nextSequenceId)
{
    nextSequenceId = await Utils.GetSequenceNumberFromPbf(index, resolution);
    Console.WriteLine($"Got {nextSequenceId} from PBF.");
}
while (true)
{
    var changeset = await DownloadDiff(resolution, nextSequenceId);
    if (changeset is null)
    {
        await Task.Delay(TimeSpan.FromMinutes(1));
        continue;
    }
    var replicationState = await resolution.GetReplicationState(nextSequenceId);
    if (replicationState is null)
    {
        throw new InvalidOperationException("How we got changeset but no replication state?");
    }
    using var tx = keyValueDatabase.BeginTransaction();
    Console.WriteLine($"Processing changeset '{nextSequenceId}' from '{replicationState.StartTimestamp}'.");
    var data = new IssuesData()
    {
        DateTime = replicationState.StartTimestamp,
        AllIssues = new List<IssueData>()
    };
    dbWithChagnes.ApplyChangeset(changeset, tx);
    foreach (var analyzer in analyzers)
    {
        Console.WriteLine($"{DateTime.Now} Starting {analyzer.AnalyzerName}.");
        var relevatThings = dbWithChagnes.Filter(analyzer.FilterSettings).ToArray();
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


string DiffUrl(ReplicationConfig config, string filePath)
{
    return new Uri(new Uri(config.Url), filePath).ToString();
}

async Task<OsmChange?> DownloadDiff(ReplicationConfig config, long sequenceNumber)
{
    var replicationFilePath = ReplicationFilePath(sequenceNumber);
    var url = DiffUrl(config, replicationFilePath);
    var cachePath = Path.Combine(@"C:\COSMOS", "ReplicationCache", config.IsDaily ? "daily" : config.IsHourly ? "hour" : "minute", replicationFilePath);
    if (!File.Exists(cachePath))
    {
        Directory.CreateDirectory(Path.GetDirectoryName(cachePath));
        using FileStream fsw = File.Create(cachePath);
        using Stream stream = await httpClient.GetStreamAsync(url);
        await stream.CopyToAsync(fsw);
    }
    using FileStream fs = File.OpenRead(cachePath);
    using GZipStream stream2 = new GZipStream(fs, CompressionMode.Decompress);
    using StreamReader textReader = new StreamReader(stream2);
    return ThreadLocalXmlSerializer.Value!.Deserialize(textReader) as OsmChange;
}

static string ReplicationFilePath(long sequenceNumber)
{
    string text = "000000000" + sequenceNumber;
    string text2 = text.Substring(text.Length - 9);
    string text3 = text2.Substring(0, 3);
    string text4 = text2.Substring(3, 3);
    string text5 = text2.Substring(6, 3);
    var filePath = $"{text3}/{text4}/{text5}.osc.gz";
    return filePath;
}