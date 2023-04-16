using OsmNightWatch;
using OsmNightWatch.Analyzers;
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

var path = @"C:\COSMOS\planet-230403.osm.pbf";
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);

var analyzers = new IOsmAnalyzer[] {
    new OsmNightWatch.Analyzers.BrokenCoastline.BrokenCoastlineAnalyzer()
};

using var database = new KeyValueDatabase(Path.GetFullPath("NightWatchDatabase"));
var dbWithChanges = new OsmDatabaseWithReplicationData(pbfDb, database);
var currentTimeStamp = database.GetTimestamp();
if (currentTimeStamp == null)
{
    currentTimeStamp = Utils.GetLatestTimestampFromPbf(index);
}
IReplicationDiffEnumerator enumerator = new CatchupReplicationDiffEnumerator((DateTime)currentTimeStamp);
IssuesData? oldIssuesData = await IssuesUploader.DownloadAsync();


while (true)
{
    var replicationState = await GetNextState(enumerator);

retry:
    try
    {
        Log($"Downloading changeset '{replicationState.EndTimestamp}'.");
        var changeset = await DownloadChangeset(replicationState.Config, replicationState.SequenceNumber);

        database.BeginTransaction();

        Log($"Applying changeset to database...");
        dbWithChanges.ApplyChangeset(changeset);

        database.SetTimestamp(replicationState.EndTimestamp);
        Log($"Analyzing changeset...");
        var newIssuesData = Analyze(analyzers, changeset, dbWithChanges, replicationState);

        newIssuesData.SetTimestampsAndLastKnownGood(oldIssuesData);
        oldIssuesData = newIssuesData;
        //await UploadIssues(replicationState, newIssuesData);
        database.CommitTransaction();
    }
    catch (Exception)
    {
        database.AbortTransaction();
        goto retry;
    }
}

void Log(string message)
{
    Console.WriteLine(DateTime.Now.ToString("s") + ": " + message);
}

async Task<ReplicationState> GetNextState(IReplicationDiffEnumerator enumerator)
{
    while (true)
    {
        try
        {
            if (await enumerator.MoveNext() == false)
            {
                Log($"Failed to iterate enumerator... Sleeping 1 minute.");
                await Task.Delay(TimeSpan.FromMinutes(1));
                if (enumerator.State.Config.Period == ReplicationConfig.Minutely.Period && enumerator is CatchupReplicationDiffEnumerator)
                {
                    enumerator = await ReplicationConfig.Minutely.GetDiffEnumerator(enumerator.State.SequenceNumber);
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
            await Task.Delay(TimeSpan.FromMinutes(1));
            continue;
        }
        return replicationState;
    }
}

string DiffUrl(ReplicationConfig config, string filePath)
{
    return new Uri(new Uri(config.Url), filePath).ToString();
}

async Task<OsmChange> DownloadChangeset(ReplicationConfig config, long sequenceNumber)
{
    bool ignoreCache = false;
    while (true)
    {
        try
        {
            var replicationFilePath = ReplicationFilePath(sequenceNumber);
            var url = DiffUrl(config, replicationFilePath);
            var cachePath = Path.Combine(@"C:\", "ReplicationCache", config.IsDaily ? "daily" : config.IsHourly ? "hour" : "minute", replicationFilePath);
            if (!ignoreCache && !File.Exists(cachePath))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(cachePath));
                using FileStream fsw = File.Create(cachePath);
                using Stream stream = await httpClient.GetStreamAsync(url);
                await stream.CopyToAsync(fsw);
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

static async Task UploadIssues(ReplicationState replicationState, IssuesData newIssuesData)
{
    // Only do uploading on changesets that are newer than 5 minutes
    if (replicationState.EndTimestamp.AddMinutes(5) > DateTime.UtcNow)
    {
        try
        {
            await IssuesUploader.UploadAsync(newIssuesData);
        }
        catch (Exception ex)
        {
            Thread.Sleep(5000);
            Console.WriteLine(ex);
        }
    }
}

static IssuesData Analyze(IOsmAnalyzer[] analyzers, OsmChange changeset, OsmDatabaseWithReplicationData dbWithChanges, ReplicationState replicationState)
{
    var newIssuesData = new IssuesData() {
        DateTime = replicationState.EndTimestamp,
        MinutelySequenceNumber = (int)replicationState.SequenceNumber
    };

    foreach (var analyzer in analyzers)
    {
        //Console.WriteLine($"{DateTime.Now} Starting filtering relevant things...");
        //var relevantThings = dbWithChanges.Filter(analyzer.FilterSettings);
        ////Console.WriteLine($"{DateTime.Now} Filtered relevant things {relevantThings.Length}.");
        //Console.WriteLine($"{DateTime.Now} Starting {analyzer.AnalyzerName}...");
        //var issues = analyzer.GetIssues(relevantThings, dbWithChanges).ToList();
        //Console.WriteLine($"{DateTime.Now} Found {issues.Count} issues.");
        //newIssuesData.AllIssues.AddRange(issues);
    }

    return newIssuesData;
}