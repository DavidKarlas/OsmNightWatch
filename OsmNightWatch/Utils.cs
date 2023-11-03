using OsmNightWatch.Analyzers.ImportantFeatures;
using OsmNightWatch.PbfParsing;
using System.Collections.Concurrent;
using System.Data.SQLite;

namespace OsmNightWatch
{
    static class Utils
    {
        public static string GetFriendlyName(this ImportantFeatureJson featureJson)
        {
            if (featureJson.tags.TryGetValue("name:en", out var nameEn))
            {
                if (nameEn is string str)
                    return str;
                else
                    return string.Join(", ", (string[])nameEn);
            }
            if (featureJson.tags.TryGetValue("name", out var name))
            {
                if (name is string str)
                    return str;
                else
                    return string.Join(", ", (string[])name);
            }
            return "Unnamed";
        }

        public static string ToChar(this OsmGeoType type)
        {
            return type switch {
                OsmGeoType.Node => "N",
                OsmGeoType.Way => "W",
                OsmGeoType.Relation => "R",
                _ => throw new NotImplementedException(),
            };
        }

        public static DateTime GetLatestTimestampFromPbf(PbfIndex pbfIndex)
        {
            var offset = pbfIndex.GetLastNodeOffset();
            var lastNodesWithMeta = NodesParser.LoadNodesWithMetadata(pbfIndex.PbfPath, offset).Last();
            if (lastNodesWithMeta.TimeStamp is not DateTime datetime)
                throw new NotSupportedException();
            return datetime;
        }

        public static void ExecuteNonQuery(this SQLiteConnection connection, string commandText)
        {
            using var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.ExecuteNonQuery();
        }

        public static SQLiteCommand CreateCommand(this SQLiteConnection connection, string commandText)
        {
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            return command;
        }

        public static void BatchLoad(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource osmSource, bool ways, bool nodes)
        {
            var allRelations = RecursivelyLoadAllRelations(relevantThings, osmSource);
            var waysToLoad = new HashSet<long>();
            var nodesToLoad = new HashSet<long>();
            foreach (var relation in allRelations)
            {
                foreach (var member in relation.Members)
                {
                    if (member.Type == OsmGeoType.Way && ways)
                        waysToLoad.Add(member.Id);
                    if (member.Type == OsmGeoType.Node && nodes)
                        nodesToLoad.Add(member.Id);
                }
            }
            osmSource.BatchLoad(wayIds: waysToLoad);
            if (nodes)
            {
                foreach (var way in relevantThings.Union(waysToLoad.Select(id => osmSource.Get(OsmGeoType.Way, id))).OfType<Way>())
                    nodesToLoad.UnionWith(way.Nodes);
                osmSource.BatchLoad(nodeIds: nodesToLoad);
            }
        }

        private static List<Relation> RecursivelyLoadAllRelations(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource osmSource)
        {
            var relationsBag = new ConcurrentBag<Relation>(relevantThings.OfType<Relation>());
            Dictionary<long, Relation> dictionaryOfLoadedRelations;
            while (true)
            {
                dictionaryOfLoadedRelations = relationsBag.ToDictionary(r => r.Id!, r => r);
                var unloadedChildren = new HashSet<long>();
                foreach (var relation in dictionaryOfLoadedRelations.Values)
                {
                    foreach (var member in relation.Members)
                    {
                        if (member.Type != OsmGeoType.Relation)
                            continue;
                        if (dictionaryOfLoadedRelations.ContainsKey(member.Id))
                            continue;
                        unloadedChildren.Add(member.Id);
                    }
                }
                if (unloadedChildren.Count == 0)
                {
                    break;
                }
                osmSource.BatchLoad(null, null, unloadedChildren);
                foreach (var relationId in unloadedChildren)
                {
                    relationsBag.Add((Relation)osmSource.Get(OsmGeoType.Relation, relationId));
                }
            }
            return relationsBag.ToList();
        }
    }
}
