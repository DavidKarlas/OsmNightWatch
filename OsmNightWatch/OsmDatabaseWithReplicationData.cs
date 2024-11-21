using OsmNightWatch.PbfParsing;

namespace OsmNightWatch
{
    public class OsmDatabaseWithReplicationData : IOsmGeoFilterableSource
    {
        private readonly IOsmGeoFilterableSource baseSource;

        private Dictionary<long, Relation?> changesetRelations = new();
        private Dictionary<long, Way?> changesetWays = new();
        private Dictionary<long, Node?> changesetNodes = new();

        private KeyValueDatabase keyValueDatabase;

        public OsmDatabaseWithReplicationData(IOsmGeoFilterableSource baseSource, KeyValueDatabase keyValueDatabase)
        {
            this.baseSource = baseSource;
            this.keyValueDatabase = keyValueDatabase;
        }

        public void ApplyChangeset(MergedChangeset changeset)
        {
            changesetNodes.Clear();
            changesetWays.Clear();
            changesetRelations.Clear();

            foreach (var change in changeset.Nodes)
            {
                changesetNodes[change.Key] = change.Value;
            }
            foreach (var change in changeset.Ways)
            {
                changesetWays[change.Key] = change.Value;
            }
            foreach (var change in changeset.Relations)
            {
                changesetRelations[change.Key] = change.Value;
            }

            keyValueDatabase.UpdateNodes(changesetNodes);
            keyValueDatabase.UpdateWays(changesetWays);
            keyValueDatabase.UpdateRelations(changesetRelations);
        }

        public void Put(OsmGeo element)
        {
            switch (element.Type)
            {
                case OsmGeoType.Node:
                    changesetNodes[element.Id] = (Node)element;
                    break;
                case OsmGeoType.Way:
                    changesetWays[element.Id] = (Way)element;
                    break;
                case OsmGeoType.Relation:
                    changesetRelations[element.Id] = (Relation)element;
                    break;
            }
        }

        public OsmGeo Get(OsmGeoType type, long id)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    if (changesetNodes.TryGetValue(id, out var node))
                    {
                        return node;
                    }
                    else if (keyValueDatabase.TryGetNode(id, out var nodeFromDb))
                    {
                        return nodeFromDb;
                    }
                    break;
                case OsmGeoType.Way:
                    if (changesetWays.TryGetValue(id, out var way))
                    {
                        return way;
                    }
                    else if (keyValueDatabase.TryGetWay(id, out var wayFromDb))
                    {
                        return wayFromDb;
                    }
                    break;
                case OsmGeoType.Relation:
                    if (changesetRelations.TryGetValue(id, out var relation))
                    {
                        return relation;
                    }
                    else if (keyValueDatabase.TryGetRelation(id, out var relationFromDb))
                    {
                        return relationFromDb;
                    }
                    break;
            }
            return baseSource.Get(type, id);
        }

        public (IReadOnlyCollection<Node> nodes, IReadOnlyCollection<Way> ways, IReadOnlyCollection<Relation> relations) BatchLoad(HashSet<long>? nodeIds, HashSet<long>? wayIds, HashSet<long>? relationIds)
        {
            var nodes = new List<Node>();
            if (nodeIds != null)
            {
                foreach (var nodeId in nodeIds.ToArray())
                {
                    if (changesetNodes.TryGetValue(nodeId, out var node))
                    {
                        nodeIds.Remove(nodeId);
                        if (node != null)
                            nodes.Add(node);
                    }
                    else if (keyValueDatabase.TryGetNode(nodeId, out var nodeFromDb))
                    {
                        nodeIds.Remove(nodeId);
                        if (nodeFromDb != null)
                            nodes.Add(nodeFromDb);
                    }
                }
            }
            var ways = new List<Way>();
            if (wayIds != null)
            {
                foreach (var wayId in wayIds.ToArray())
                {
                    if (changesetWays.TryGetValue(wayId, out var way))
                    {
                        wayIds.Remove(wayId);
                        if (way != null)
                            ways.Add(way);
                    }
                    else if (keyValueDatabase.TryGetWay(wayId, out var wayFromDb))
                    {
                        wayIds.Remove(wayId);
                        if (wayFromDb != null)
                            ways.Add(wayFromDb);
                    }
                }
            }
            var relations = new List<Relation>();
            if (relationIds != null)
            {
                foreach (var relationId in relationIds.ToArray())
                {
                    if (changesetRelations.TryGetValue(relationId, out var relation))
                    {
                        relationIds.Remove(relationId);
                        if (relation != null)
                            relations.Add(relation);
                    }
                    else if (keyValueDatabase.TryGetRelation(relationId, out var relationFromDb))
                    {
                        relationIds.Remove(relationId);
                        if (relationFromDb != null)
                            relations.Add(relationFromDb);
                    }
                }
            }
            var baseResults = baseSource.BatchLoad(nodeIds, wayIds, relationIds);
            nodes.AddRange(baseResults.nodes);
            ways.AddRange(baseResults.ways);
            relations.AddRange(baseResults.relations);
            return (nodes, ways, relations);
        }

        public void ClearBatchCache()
        {
            baseSource.ClearBatchCache();
        }

        public void StoreCache()
        {
            keyValueDatabase.UpdateNodes(changesetNodes, true);
            keyValueDatabase.UpdateWays(changesetWays, true);
            keyValueDatabase.UpdateRelations(changesetRelations, true);
        }

        public Node GetNode(long id)
        {
            return Get(OsmGeoType.Node, id) as Node;
        }

        public Way GetWay(long id)
        {
            return Get(OsmGeoType.Way, id) as Way;
        }

        public Relation GetRelation(long id)
        {
            return Get(OsmGeoType.Relation, id) as Relation;
        }

        public IEnumerable<OsmGeo> Filter(FilterSettings filterSettings)
        {
            foreach (var element in baseSource.Filter(filterSettings))
            {
                Put(element);
                yield return element;
            }
        }
    }
}
