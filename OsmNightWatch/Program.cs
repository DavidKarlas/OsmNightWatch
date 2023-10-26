using MonoTorrent.Client;
using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.AdminCountPerCountry;
using OsmNightWatch.Analyzers.BrokenCoastline;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using OsmSharp.Changesets;
using OsmSharp.Replication;
using System.IO.Compression;
using System.Xml.Serialization;

HttpClient httpClient = new HttpClient();
ThreadLocal<XmlSerializer> ThreadLocalXmlSerializer = new ThreadLocal<XmlSerializer>(() => new XmlSerializer(typeof(OsmChange)));
var dataStoragePath = Path.GetFullPath("NightWatchDatabase");
Directory.CreateDirectory(dataStoragePath);
var path = Directory.GetFiles(dataStoragePath, "planet-*.osm.pbf").OrderBy(f => f).LastOrDefault();
if (path == null || !PbfIndexBuilder.DoesIndexExist(path))
{
    using var engine = new ClientEngine();
    using var torrentStream = await httpClient.GetStreamAsync("https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent");
    using var memoryStream = new MemoryStream();
    await torrentStream.CopyToAsync(memoryStream);
    memoryStream.Position = 0;
    await engine.AddAsync(MonoTorrent.Torrent.Load(memoryStream), dataStoragePath);
    await engine.StartAllAsync();
    while (engine.IsRunning)
    {
        var torrent = engine.Torrents.Single();
        Console.WriteLine($"Downloading {engine.Torrents.Single().Files.Single().Path} {torrent.Progress:0.00}% with speed {torrent.Monitor.DownloadRate / 1024.0 / 1024.0:0.00} MB/s.");
        if (torrent.Complete)
            break;
        await Task.Delay(10_000);
    }
    path = engine.Torrents.Single().Files.Single().FullPath;
}

using var database = new KeyValueDatabase(dataStoragePath);
database.BeginTransaction();

var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var analyzers = new IOsmAnalyzer[] {
    new AdminCountPerCountryAnalyzer(database, dataStoragePath),
    new BrokenCoastlineAnalyzer(database, dataStoragePath),
    new OsmNightWatch.Analyzers.ImportantFeatures.ImportantFeaturesAnalyzer(dataStoragePath)
};

var dbWithChanges = new OsmDatabaseWithReplicationData(pbfDb, database);
var currentTimeStamp = database.GetTimestamp();
if (currentTimeStamp == null)
{
    foreach (var analyzer in analyzers)
    {
        var relevantThings = dbWithChanges.Filter(analyzer.FilterSettings).ToArray();
        Log($"Starting {analyzer.AnalyzerName}...");
        var issues = analyzer.ProcessPbf(relevantThings, dbWithChanges);
        Log($"Found {issues.Count()} issues.");
    }
    Log("Storing relevant elements into LMDB.");
    dbWithChanges.StoreCache();
    Log("Finished storing relevant elements into LMDB.");
    currentTimeStamp = Utils.GetLatestTimestampFromPbf(index);
    database.SetTimestamp((DateTime)currentTimeStamp);
    database.CommitTransaction();
    Log("Committed transaction.");
}
else
{
    database.AbortTransaction();
}
IssuesData? oldIssuesData = IssuesUploader.Download();


while (true)
{
retry:
    try
    {
        var enumerator = new CatchupReplicationDiffEnumerator((DateTime)currentTimeStamp);

        var mergedChangeset = new MergedChangeset();
        ReplicationState? replicationState = null;
        while (enumerator.MoveNext().Result == true)
        {
            replicationState = enumerator.State;
            Log($"Downloading changeset '{replicationState.EndTimestamp}'.");
            mergedChangeset.Add(DownloadChangeset(replicationState.Config, replicationState.SequenceNumber));
        }
        if (replicationState == null)
        {
            await Task.Delay(60 * 1000);
            continue;
        }
        mergedChangeset.Build();

        database.BeginTransaction();
        Log($"Applying changeset to database...");
        dbWithChanges.ApplyChangeset(mergedChangeset);

        database.SetTimestamp(replicationState.EndTimestamp);
        Log($"Analyzing changeset...");
        var newIssuesData = Analyze(analyzers, mergedChangeset, dbWithChanges, replicationState);

        newIssuesData.SetTimestampsAndLastKnownGood(oldIssuesData);
        oldIssuesData = newIssuesData;
        UploadIssues(replicationState, newIssuesData);
        currentTimeStamp = replicationState.EndTimestamp;
        database.CommitTransaction();
    }
    catch (Exception ex)
    {
        Log(ex.ToString());
        database.AbortTransaction();
        goto retry;
    }
}

static void Log(string message)
{
    Console.WriteLine(DateTime.Now.ToString("s") + ": " + message);
}

string DiffUrl(ReplicationConfig config, string filePath)
{
    return new Uri(new Uri(config.Url), filePath).ToString();
}

OsmChange DownloadChangeset(ReplicationConfig config, long sequenceNumber)
{
    bool ignoreCache = false;
    while (true)
    {
        try
        {
            var replicationFilePath = ReplicationFilePath(sequenceNumber);
            var url = DiffUrl(config, replicationFilePath);
            var cachePath = Path.Combine(@"C:\", "ReplicationCache", config.IsDaily ? "daily" : config.IsHourly ? "hour" : "minute", replicationFilePath);
            if (ignoreCache || !File.Exists(cachePath))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(cachePath));
                using FileStream fsw = File.Create(cachePath);
                using Stream stream = httpClient.GetStreamAsync(url).Result;
                stream.CopyTo(fsw);
            }
            Log("Deserializing changeset...");
            using FileStream fs = File.OpenRead(cachePath);
            using GZipStream stream2 = new GZipStream(fs, CompressionMode.Decompress);
            using StreamReader textReader = new StreamReader(stream2);
            var changeset = ThreadLocalXmlSerializer.Value!.Deserialize(textReader) as OsmChange;
            if (changeset is null)
            {
                throw new InvalidOperationException("How we got replication state but no changeset?");
            }
            return changeset;
        }
        catch (Exception ex)
        {
            ignoreCache = true;
            Log("Failed to download/deserialize changeset: " + ex);
        }
    }
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

static void UploadIssues(ReplicationState replicationState, IssuesData newIssuesData)
{
    // Only do uploading on changesets that are newer than 5 minutes
    if (replicationState.EndTimestamp.AddMinutes(5) > DateTime.UtcNow ||
        (IssuesUploader.Download() is IssuesData existingData && existingData.DateTime < newIssuesData.DateTime))
    {
        try
        {
            IssuesUploader.Upload(newIssuesData);
        }
        catch (Exception ex)
        {
            Thread.Sleep(5000);
            Console.WriteLine(ex);
        }
    }
}

static IssuesData Analyze(IOsmAnalyzer[] analyzers, MergedChangeset mergedChangeset, OsmDatabaseWithReplicationData dbWithChanges, ReplicationState replicationState)
{
    var newIssuesData = new IssuesData()
    {
        DateTime = replicationState.EndTimestamp,
        MinutelySequenceNumber = (int)replicationState.SequenceNumber
    };

    foreach (var analyzer in analyzers)
    {
        Log($"Starting {analyzer.AnalyzerName}...");
        var issues = analyzer.ProcessChangeset(mergedChangeset, dbWithChanges).ToList();
        Log($"Found {issues.Count} issues.");
        newIssuesData.AllIssues.AddRange(issues);
    }

    return newIssuesData;
}