using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using OsmSharp.Changesets;
using OsmSharp.Replication;
using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Serialization;

HttpClient httpClient = new HttpClient();
ThreadLocal<XmlSerializer> ThreadLocalXmlSerializer = new ThreadLocal<XmlSerializer>(() => new XmlSerializer(typeof(OsmChange)));

var path = @"C:\COSMOS\planet-230403.osm.pbf";
bool pbfOnly = true;
var index = PbfIndexBuilder.BuildIndex(path);
var pbfDb = new PbfDatabase(index);
var analyzers = new IOsmAnalyzer[] {
    //new OsmNightWatch.Analyzers.AdminCountPerAdmin2.AdminCountPerAdmin2Analyzer(),
    //new OsmNightWatch.Analyzers.OpenPolygon.AdminOpenPolygonAnalyzer(),
    //new OsmNightWatch.Analyzers.BrokenCoastline.BrokenCoastlineAnalyzer()
    };
var inrcementalAnalyzers = new IIncrementalOsmAnalyzer[] {
    new bla()
};

var sw = Stopwatch.StartNew();
foreach (var analyzer in inrcementalAnalyzers)
{
    var validator = analyzer.GetValidator();
    var issues = pbfDb.Validate(validator, analyzer.FilterSettings);
}

Console.WriteLine(sw.Elapsed);
return;

var dbWithChanges = new OsmDatabaseWithReplicationData(pbfDb);
var pbfTimestamp = Utils.GetLatestTimestampFromPbf(index);
Console.WriteLine($"PBF timestamp {pbfTimestamp}.");
IReplicationDiffEnumerator enumerator = new CatchupReplicationDiffEnumerator(pbfTimestamp);
IssuesData? oldIssuesData = pbfOnly ? null : await IssuesUploader.DownloadAsync();
while (true)
{
    ReplicationState? replicationState = null;
retryEnumerator:
    if (!pbfOnly)
    {
        try
        {
            if (await enumerator.MoveNext() == false)
            {
                Console.WriteLine($"Failed to iterate enumerator... Sleeping 1 minute.");
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
            Console.WriteLine(ex);
            goto retryEnumerator;
        }
        replicationState = enumerator.State;
        if (replicationState is null)
        {
            Console.WriteLine($"Enumerator returned null state, sleeping 1 minute.");
            await Task.Delay(TimeSpan.FromMinutes(1));
            continue;
        }
    }
retryProcessing:
    try
    {
        if (!pbfOnly)
        {
            Console.WriteLine($"Downloading changeset '{replicationState.SequenceNumber}'.");
            var changeset = await DownloadDiff(replicationState.Config, replicationState.SequenceNumber);
            if (changeset is null)
            {
                throw new InvalidOperationException("How we got changeset but no replication state?");
            }
            Console.WriteLine($"Processing changeset '{replicationState.Config.Url}' '{replicationState.SequenceNumber}' from '{replicationState.StartTimestamp}'.");

            dbWithChanges.ApplyChangeset(changeset);
        }
        // Only do processing old changesets newer than already processed data..
        if (pbfOnly || replicationState!.StartTimestamp > oldIssuesData!.DateTime)
        {
            var newIssuesData = new IssuesData() {
                DateTime = replicationState?.EndTimestamp ?? default,
                MinutelySequenceNumber = (int)(replicationState?.SequenceNumber ?? 0)
            };

            foreach (var analyzer in analyzers)
            {
                Console.WriteLine($"{DateTime.Now} Starting filtering relevant things...");
                var relevantThings = dbWithChanges.Filter(analyzer.FilterSettings);
                //Console.WriteLine($"{DateTime.Now} Filtered relevant things {relevantThings.Length}.");
                Console.WriteLine($"{DateTime.Now} Starting {analyzer.AnalyzerName}...");
                var issues = analyzer.GetIssues(relevantThings, dbWithChanges).ToList();
                Console.WriteLine($"{DateTime.Now} Found {issues.Count} issues.");
                newIssuesData.AllIssues.AddRange(issues);
            }

            newIssuesData.SetTimestampsAndLastKnownGood(oldIssuesData);
            oldIssuesData = newIssuesData;

#if !DEBUG
        retryUpload:
            //Only do uploading on changesets that are 5 minutes or newer
            if (!pbfOnly && replicationState!.EndTimestamp.AddMinutes(5) > DateTime.UtcNow)
            {
                try
                {
                    await IssuesUploader.UploadAsync(newIssuesData);
                }
                catch (Exception ex)
                {
                    Thread.Sleep(5000);
                    Console.WriteLine(ex);
                    goto retryUpload;
                }
            }
#endif
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex);
        goto retryProcessing;
    }
}


string DiffUrl(ReplicationConfig config, string filePath)
{
    return new Uri(new Uri(config.Url), filePath).ToString();
}

async Task<OsmChange?> DownloadDiff(ReplicationConfig config, long sequenceNumber)
{
    var replicationFilePath = ReplicationFilePath(sequenceNumber);
    var url = DiffUrl(config, replicationFilePath);
    var cachePath = Path.Combine(@"C:\", "ReplicationCache", config.IsDaily ? "daily" : config.IsHourly ? "hour" : "minute", replicationFilePath);
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