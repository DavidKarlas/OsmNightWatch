using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace OsmNightWatch.Analyzers.ImportantFeatures
{
    public class ImportantFeaturesAnalyzer : IOsmAnalyzer
    {
        public string AnalyzerName => nameof(ImportantFeaturesAnalyzer);

        public FilterSettings FilterSettings { get; private set; }

        Dictionary<long, ImportantFeatureJson> Nodes = new();
        Dictionary<long, ImportantFeatureJson> Ways = new();
        Dictionary<long, ImportantFeatureJson> Relations = new();

        Dictionary<OsmGeoType, Dictionary<long, IssueData[]>> Issues = new() {
            { OsmGeoType.Node, new() },
            { OsmGeoType.Way, new() },
            { OsmGeoType.Relation, new() }
        };
        private readonly string storagePath;

        private void Store()
        {
            File.WriteAllText(storagePath, JsonSerializer.Serialize(Issues));
        }

        public ImportantFeaturesAnalyzer(string dataStoragePath)
        {
            storagePath = Path.Combine(dataStoragePath, "importantFeatures.json");
            if (File.Exists(storagePath))
            {
                Issues = JsonSerializer.Deserialize<Dictionary<OsmGeoType, Dictionary<long, IssueData[]>>>(File.ReadAllText(storagePath))!;
            }
            UpdateFilterSettings();
        }

        HttpClient httpClient = new HttpClient();
        Stopwatch? cacheStopwatch;
        private void UpdateFilterSettings()
        {
            if (cacheStopwatch != null && cacheStopwatch.Elapsed < TimeSpan.FromHours(1))
            {
                return;
            }
            cacheStopwatch = Stopwatch.StartNew();
            using var json = httpClient.GetStreamAsync("https://daylight-map-distribution.s3.us-west-1.amazonaws.com/release/v1.33/important-features-v1.33.json").Result;
            var parsedJson = JsonSerializer.Deserialize<ImportantFeatureJson[]>(json)!;
            foreach (var feature in parsedJson)
            {
                feature.tags = feature.tags.ToDictionary(c => c.Key, c => ConvertToStr((JsonElement)c.Value));
            }
            FilterSettings = new FilterSettings() {
                Filters = new(){
                    new ElementFilter(OsmGeoType.Node, ids: parsedJson.Where(c=>c.osm_type ==  OsmGeoType.Node).Select(c=>c.osm_id)),
                    new ElementFilter(OsmGeoType.Way, ids: parsedJson.Where(c=>c.osm_type ==  OsmGeoType.Way).Select(c=>c.osm_id)),
                    new ElementFilter(OsmGeoType.Relation, ids: parsedJson.Where(c=>c.osm_type == OsmGeoType.Relation).Select(c=>c.osm_id))
                }
            };
            Nodes = parsedJson.Where(c => c.osm_type == OsmGeoType.Node).ToDictionary(c => c.osm_id);
            Ways = parsedJson.Where(c => c.osm_type == OsmGeoType.Way).ToDictionary(c => c.osm_id);
            Relations = parsedJson.Where(c => c.osm_type == OsmGeoType.Relation).ToDictionary(c => c.osm_id);
        }

        private object ConvertToStr(JsonElement value)
        {
            if (value.ValueKind == JsonValueKind.String)
                return value.GetString()!;
            return value.EnumerateArray().Select(c => c.GetString()).ToArray();
        }

        public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoBatchSource newOsmSource)
        {
            UpdateFilterSettings();
            foreach (var node in changeSet.Nodes)
            {
                if (Nodes.TryGetValue(node.Key, out var featureSpec))
                {
                    Validate(node.Key, node.Value, featureSpec);
                }
            }
            foreach (var way in changeSet.Ways)
            {
                if (Ways.TryGetValue(way.Key, out var featureSpec))
                {
                    Validate(way.Key, way.Value, featureSpec);
                }
            }
            foreach (var relation in changeSet.Relations)
            {
                if (Relations.TryGetValue(relation.Key, out var featureSpec))
                {
                    Validate(relation.Key, relation.Value, featureSpec);
                }
            }
            Store();
            return Issues.Values.SelectMany(c => c.Values).SelectMany(c => c);
        }


        private void Validate(long id, OsmGeo? element, ImportantFeatureJson featureSpec)
        {
            if (element == null)
            {
                Issues[featureSpec.osm_type][id] = new[]{new IssueData() {
                    OsmType = featureSpec.osm_type.ToChar(),
                    OsmId = id,
                    Details = "Deleted",
                    IssueType = "ImportantFeatures",
                    FriendlyName = featureSpec.GetFriendlyName()
                }};
                return;
            }
            var issues = new List<IssueData>();
            foreach (var expectedTag in featureSpec.tags)
            {
                if (expectedTag.Key == "population")
                    continue;

                if (!(element.Tags?.TryGetValue(expectedTag.Key, out var actualTag) ?? false))
                {
                    issues.Add(new IssueData() {
                        OsmType = featureSpec.osm_type.ToChar(),
                        OsmId = id,
                        Details = $"Missing tag '{expectedTag.Key}', expected it to be '{FormatOr(expectedTag.Value)}'",
                        IssueType = "ImportantFeatures",
                        FriendlyName = featureSpec.GetFriendlyName()
                    });
                    continue;
                }
                switch (expectedTag.Value)
                {
                    case string str:
                        if (actualTag != str)
                        {
                            issues.Add(new IssueData() {
                                OsmType = featureSpec.osm_type.ToChar(),
                                OsmId = id,
                                Details = $"Expected tag '{expectedTag.Key}' to be '{expectedTag.Value}', but found '{actualTag}'",
                                IssueType = "ImportantFeatures",
                                FriendlyName = featureSpec.GetFriendlyName()
                            });
                        }
                        break;
                    case string[] strArr:
                        if (!strArr.Contains(actualTag))
                        {
                            issues.Add(new IssueData() {
                                OsmType = featureSpec.osm_type.ToChar(),
                                OsmId = id,
                                Details = $"Expected tag '{expectedTag.Key}' to be {FormatOr(strArr)}, but found '{actualTag}'",
                                IssueType = "ImportantFeatures",
                                FriendlyName = featureSpec.GetFriendlyName()
                            });
                        }
                        break;
                }
            }
            if (issues.Count > 0)
            {
                Issues[featureSpec.osm_type][id] = issues.ToArray();
            }
            else
            {
                Issues[featureSpec.osm_type].Remove(id);
            }
        }

        private object FormatOr(object val)
        {
            if (val is not string[] strArr)
                return $"'{val}'";
            if (strArr.Length == 1)
                return $"'{strArr[0]}'";
            if (strArr.Length == 2)
                return $"'{strArr[0]}' or '{strArr[1]}'";
            return $"'{string.Join("', '", strArr.Take(strArr.Length - 1))}', or '{strArr.Last()}'";
        }

        public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
        {
            foreach (var relevantThing in relevantThings)
            {
                switch (relevantThing.Type)
                {
                    case OsmGeoType.Node:
                        if (Nodes.TryGetValue(relevantThing.Id, out var spec))
                        {
                            Validate(relevantThing.Id, relevantThing, spec);
                        }
                        else
                        {
                            throw new InvalidOperationException();
                        }
                        break;
                    case OsmGeoType.Way:
                        if (Ways.TryGetValue(relevantThing.Id, out var waySpec))
                        {
                            Validate(relevantThing.Id, relevantThing, waySpec);
                        }
                        else
                        {
                            throw new InvalidOperationException();
                        }
                        break;
                    case OsmGeoType.Relation:
                        if (Relations.TryGetValue(relevantThing.Id, out var relationSpec))
                        {
                            Validate(relevantThing.Id, relevantThing, relationSpec);
                        }
                        else
                        {
                            throw new InvalidOperationException();
                        }
                        break;
                }
            }
            return Issues.Values.SelectMany(c => c.Values).SelectMany(c => c);
        }
    }
}
