using OsmNightWatch.PbfParsing;
using OsmSharp;
using OsmSharp.Replication;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    static class Utils
    {
        public static Task<long> GetSequenceNumberFromPbf(PbfIndex pbfIndex)
        {
            var offset = pbfIndex.GetLastNodeOffset();
            var lastNodesWithMeta = NodesParser.LoadNodesWithMetadata(pbfIndex.PbfPath, offset).Last();
            if (lastNodesWithMeta.TimeStamp is not DateTime datetime)
                throw new NotSupportedException();
            return ReplicationConfig.Minutely.GuessSequenceNumberAt(datetime);
        }

        public static void BatchLoad(IEnumerable<OsmGeo> relevatThings, IOsmGeoBatchSource osmSource, bool ways, bool nodes)
        {
            var allRelations = RecursivlyLoadAllRelations(relevatThings, osmSource);
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
                foreach (var way in relevatThings.Union(waysToLoad.Select(id => osmSource.Get(OsmGeoType.Way, id))).OfType<Way>())
                    nodesToLoad.UnionWith(way.Nodes);
                osmSource.BatchLoad(nodeIds: nodesToLoad);
            }
        }

        private static List<Relation> RecursivlyLoadAllRelations(IEnumerable<OsmGeo> relevatThings, IOsmGeoBatchSource osmSource)
        {
            var relationsBag = new ConcurrentBag<Relation>(relevatThings.OfType<Relation>());
            Dictionary<long, Relation> dictionaryOfLoadedRelations;
            while (true)
            {
                dictionaryOfLoadedRelations = relationsBag.ToDictionary(r => (long)r.Id!, r => r);
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
