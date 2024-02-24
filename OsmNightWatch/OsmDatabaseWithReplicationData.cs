using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch
{
    public class OsmDatabaseWithReplicationData : IOsmValidateSource
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
            var (nodes, ways, relations) = baseSource.BatchLoad(nodeIds, wayIds, relationIds);
            foreach (var node in nodes)
            {
                Put(node);
            }
            foreach (var way in ways)
            {
                Put(way);
            }
            foreach (var relation in relations)
            {
                Put(relation);
            }

            return (nodes, ways, relations);
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

        public void Validate(Action<OsmGeo> validator, FilterSettings filterSettings)
        {
            ((IOsmValidateSource)baseSource).Validate(validator, filterSettings);
        }
    }
}
