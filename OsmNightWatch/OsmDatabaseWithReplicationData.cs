using FASTER.core;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;

namespace OsmNightWatch
{
    public class OsmDatabaseWithReplicationData : IOsmGeoSource
    {
        private readonly IOsmGeoSource baseSource;

        private Dictionary<long, Relation?> changesetRelations = new();
        private Dictionary<long, Way?> changesetWays = new();
        private Dictionary<long, Node?> changesetNodes = new();

        private KeyValueDatabase keyValueDatabase;

        public OsmDatabaseWithReplicationData(IOsmGeoSource baseSource, KeyValueDatabase keyValueDatabase)
        {
            this.baseSource = baseSource;
            this.keyValueDatabase = keyValueDatabase;
        }

        public void ApplyChangeset(OsmChange changeset)
        {
            changesetNodes.Clear();
            changesetWays.Clear();
            changesetRelations.Clear();

            foreach (var change in changeset.Create)
            {
                Put(change);
            }
            foreach (var change in changeset.Modify)
            {
                Put(change);
            }
            foreach (var change in changeset.Delete)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                        {
                            if (changesetNodes.TryGetValue(idNode, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            changesetNodes[idNode] = null;
                        }
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                        {
                            if (changesetWays.TryGetValue(idWay, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            changesetWays[idWay] = null;
                        }
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                        {
                            if (changesetRelations.TryGetValue(idRelation, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            changesetRelations[idRelation] = null;
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }

            keyValueDatabase.UpdateNodes(changesetNodes);
            keyValueDatabase.UpdateWays(changesetWays);
            keyValueDatabase.UpdateRelations(changesetRelations);
        }

        private void Put(OsmGeo element)
        {
            switch (element.Type)
            {
                case OsmGeoType.Node:
                    changesetNodes[(long)element.Id!] = (Node)element;
                    break;
                case OsmGeoType.Way:
                    changesetWays[(long)element.Id!] = (Way)element;
                    break;
                case OsmGeoType.Relation:
                    changesetRelations[(long)element.Id!] = (Relation)element;
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
    }
}
