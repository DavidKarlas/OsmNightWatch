using OsmNightWatch.Lib;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;

namespace OsmNightWatch
{
    public class OsmDatabaseWithReplicationData : IOsmGeoFilterableSource
    {
        private readonly IOsmGeoSource baseSource;

        private Dictionary<long, Relation?> changesetRelations = new();
        private Dictionary<long, Way?> changesetWays = new();
        private Dictionary<long, Node?> changesetNodes = new();

        public OsmDatabaseWithReplicationData(IOsmGeoSource baseSource)
        {
            this.baseSource = baseSource;
        }

        public void ApplyChangeset(OsmChange changeset)
        {
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

        public void BatchLoad(HashSet<long>? nodeIds, HashSet<long>? wayIds, HashSet<long>? relationIds)
        {
            if (nodeIds != null)
            {
                nodeIds = new HashSet<long>(nodeIds);
                nodeIds.ExceptWith(changesetNodes.Keys);
            }
            if (wayIds != null)
            {
                wayIds = new HashSet<long>(wayIds);
                wayIds.ExceptWith(changesetWays.Keys);
            }
            if (relationIds != null)
            {
                relationIds = new HashSet<long>(relationIds);
                relationIds.ExceptWith(changesetRelations.Keys);
            }
            (baseSource as IOsmGeoBatchSource)?.BatchLoad(nodeIds, wayIds, relationIds);
        }

        Dictionary<FilterSettings, IEnumerable<OsmGeo>> _cache = new();

        public IEnumerable<OsmGeo> Filter(FilterSettings filterSettings)
        {
            var filters = filterSettings.Filters;
            var nodeFilter = filters.Where(f => f.GeoType == OsmGeoType.Node).SingleOrDefault();
            var wayFilter = filters.Where(f => f.GeoType == OsmGeoType.Way).SingleOrDefault();
            var relationFilter = filters.Where(f => f.GeoType == OsmGeoType.Relation).SingleOrDefault();

            if (baseSource is IOsmGeoFilterableSource baseFilterableSource)
            {
                if (!_cache.TryGetValue(filterSettings, out var results))
                {
                    _cache[filterSettings] = results = baseFilterableSource.Filter(filterSettings);
                }

                foreach (var baseResult in results)
                {
                    switch (baseResult)
                    {
                        case Node node:
                            if (!changesetNodes.ContainsKey(node.Id!.Value))
                                yield return node;
                            break;
                        case Way way:
                            if (!changesetWays.ContainsKey(way.Id!.Value))
                                yield return way;
                            break;
                        case Relation relation:
                            if (!changesetRelations.ContainsKey(relation.Id!.Value))
                                yield return relation;
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
            }
            if (nodeFilter != null)
            {
                foreach (var node in changesetNodes.Values)
                {
                    if (node != null && nodeFilter.Matches(node))
                        yield return node;
                }
            }
            if (wayFilter != null)
            {
                foreach (var way in changesetWays.Values)
                {
                    if (way != null && wayFilter.Matches(way))
                        yield return way;
                }
            }
            if (relationFilter != null)
            {
                foreach (var relation in changesetRelations.Values)
                {
                    if (relation != null && relationFilter.Matches(relation))
                        yield return relation;
                }
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
                    break;
                case OsmGeoType.Way:
                    if (changesetWays.TryGetValue(id, out var way))
                    {
                        return way;
                    }
                    break;
                case OsmGeoType.Relation:
                    if (changesetRelations.TryGetValue(id, out var relation))
                    {
                        return relation;
                    }
                    break;
            }
            return baseSource.Get(type, id);
        }
    }
}
