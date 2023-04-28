using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Analyzers.AdminCountPerCountry;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using OsmSharp.Changesets;
using OsmSharp.IO.PBF;
using OsmSharp.Replication;
using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Serialization;

HttpClient httpClient = new HttpClient();
ThreadLocal<XmlSerializer> ThreadLocalXmlSerializer = new ThreadLocal<XmlSerializer>(() => new XmlSerializer(typeof(OsmChange)));

Log("Hello");

var path = @"C:\COSMOS\planet-230403.osm.pbf";

using var database = new KeyValueDatabase(Path.GetFullPath("NightWatchDatabase"));
database.Initialize();
database.BeginTransaction();

var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var analyzers = new IOsmAnalyzer[] {
    new AdminCountPerCountryAnalyzer(database)
};

var dbWithChanges = new OsmDatabaseWithReplicationData(pbfDb, database);
var currentTimeStamp = database.GetTimestamp();
if (currentTimeStamp == null)
{
    foreach (var analyzer in analyzers)
    {
        var relevantThings = dbWithChanges.Filter(analyzer.FilterSettings).ToArray();
        Console.WriteLine(analyzer.ProcessPbf(relevantThings, dbWithChanges).Count());
    }
    dbWithChanges.StoreCache();
    database.CommitTransaction();
    currentTimeStamp = Utils.GetLatestTimestampFromPbf(index);
}
else
{
    database.AbortTransaction();
}
IReplicationDiffEnumerator enumerator = new CatchupReplicationDiffEnumerator((DateTime)currentTimeStamp);
IssuesData? oldIssuesData = IssuesUploader.Download();


while (true)
{
    var replicationState = GetNextState();

retry:
    try
    {
        Log($"Downloading changeset '{replicationState.EndTimestamp}'.");
        var changeset = DownloadChangeset(replicationState.Config, replicationState.SequenceNumber);

        database.BeginTransaction();

        var mergedChangeset = new MergedChangeset(changeset);
        Log($"Applying changeset to database...");
        dbWithChanges.ApplyChangeset(mergedChangeset);

        database.SetTimestamp(replicationState.EndTimestamp);
        Log($"Analyzing changeset...");
        var newIssuesData = Analyze(analyzers, mergedChangeset, dbWithChanges, replicationState);

        newIssuesData.SetTimestampsAndLastKnownGood(oldIssuesData);
        oldIssuesData = newIssuesData;
        UploadIssues(replicationState, newIssuesData);
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

ReplicationState GetNextState()
{
    while (true)
    {
        try
        {
            if (enumerator.MoveNext().Result == false)
            {
                Log($"Failed to iterate enumerator... Sleeping 1 minute.");
                Task.Delay(TimeSpan.FromMinutes(1)).Wait();
                if (enumerator.State.Config.Period == ReplicationConfig.Minutely.Period && enumerator is CatchupReplicationDiffEnumerator)
                {
                    enumerator = ReplicationConfig.Minutely.GetDiffEnumerator(enumerator.State.SequenceNumber).Result;
                }
                continue;
            }
        }
        catch (Exception ex)
        {
            Log(ex.ToString());
            continue;
        }
        var replicationState = enumerator.State;
        if (replicationState is null)
        {
            Log($"Enumerator returned null state, sleeping 1 minute.");
            Task.Delay(TimeSpan.FromMinutes(1)).Wait();
            continue;
        }
        return replicationState;
    }
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
    if (replicationState.EndTimestamp.AddMinutes(5) > DateTime.UtcNow)
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
    var newIssuesData = new IssuesData() {
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