//using OsmSharp;
//using OsmSharp.Changesets;
//using OsmSharp.Db;

//namespace OsmNightWatch
//{
//    public class OsmDatabaseWithChangeset : IOsmGeoFilterableSource
//    {
//        private readonly IOsmGeoSource baseSource;
//        private Dictionary<long, Relation?> changesetRelations = new();
//        private Dictionary<long, Way?> changesetWays = new();
//        private Dictionary<long, Node?> changesetNodes = new();

//        public OsmDatabaseWithChangeset(IOsmGeoSource baseSource)
//        {
//            this.baseSource = baseSource;
//        }

//        public void AppyChangeset(OsmChange changeset)
//        {
//            foreach (var change in changeset.Delete)
//            {
//                switch (change.Type)
//                {
//                    case OsmGeoType.Node:
//                        if (change.Id is long idNode)
//                            changesetNodes[idNode] = null;
//                        break;
//                    case OsmGeoType.Way:
//                        if (change.Id is long idWay)
//                            changesetWays[idWay] = null;
//                        break;
//                    case OsmGeoType.Relation:
//                        if (change.Id is long idRelation)
//                            changesetRelations[idRelation] = null;
//                        break;
//                    default:
//                        throw new NotImplementedException();
//                }
//            }
//            foreach (var change in changeset.Modify)
//            {
//                switch (change.Type)
//                {
//                    case OsmGeoType.Node:
//                        if (change.Id is long idNode)
//                            changesetNodes[idNode] = change as Node;
//                        break;
//                    case OsmGeoType.Way:
//                        if (change.Id is long idWay)
//                            changesetWays[idWay] = change as Way;
//                        break;
//                    case OsmGeoType.Relation:
//                        if (change.Id is long idRelation)
//                            changesetRelations[idRelation] = change as Relation;
//                        break;
//                    default:
//                        throw new NotImplementedException();
//                }
//            }
//            foreach (var change in changeset.Create)
//            {
//                switch (change.Type)
//                {
//                    case OsmGeoType.Node:
//                        if (change.Id is long idNode)
//                            changesetNodes[idNode] = change as Node;
//                        break;
//                    case OsmGeoType.Way:
//                        if (change.Id is long idWay)
//                            changesetWays[idWay] = change as Way;
//                        break;
//                    case OsmGeoType.Relation:
//                        if (change.Id is long idRelation)
//                            changesetRelations[idRelation] = change as Relation;
//                        break;
//                    default:
//                        throw new NotImplementedException();
//                }
//            }
//        }

//        public void BatchLoad(HashSet<long>? nodeIds = null, HashSet<long>? wayIds = null, HashSet<long>? relationIds = null)
//        {
//            (baseSource as IOsmGeoBatchSource)?.BatchLoad(nodeIds, wayIds, relationIds);
//        }

//        public IEnumerable<OsmGeo> Filter(FilterSettings filterSettings)
//        {
//            var filters = filterSettings.Filters;
//            var nodeFilter = filters.Where(f => f.GeoType == OsmGeoType.Node).SingleOrDefault();
//            var wayFilter = filters.Where(f => f.GeoType == OsmGeoType.Way).SingleOrDefault();
//            var relationFilter = filters.Where(f => f.GeoType == OsmGeoType.Relation).SingleOrDefault();
//            if (baseSource is IOsmGeoFilterableSource baseFilterableSource)
//            {
//                foreach (var baseResult in baseFilterableSource.Filter(filterSettings))
//                {
//                    switch (baseResult)
//                    {
//                        case Node node:
//                            if (!changesetNodes.ContainsKey(node.Id!.Value))
//                                yield return node;
//                            break;
//                        case Way way:
//                            if (!changesetWays.ContainsKey(way.Id!.Value))
//                                yield return way;
//                            break;
//                        case Relation relation:
//                            if (!changesetRelations.ContainsKey(relation.Id!.Value))
//                                yield return relation;
//                            break;
//                        default:
//                            throw new NotImplementedException();
//                    }
//                }
//            }
//            if (nodeFilter != null)
//            {
//                foreach (var node in changesetNodes.Values)
//                {
//                    if (node != null && nodeFilter.Matches(node))
//                        yield return node;
//                }
//            }
//            if (wayFilter != null)
//            {
//                foreach (var way in changesetWays.Values)
//                {
//                    if (way != null && wayFilter.Matches(way))
//                        yield return way;
//                }
//            }
//            if (relationFilter != null)
//            {
//                foreach (var relation in changesetRelations.Values)
//                {
//                    if (relation != null && relationFilter.Matches(relation))
//                        yield return relation;
//                }
//            }
//        }

//        public OsmGeo Get(OsmGeoType type, long id)
//        {
//            switch (type)
//            {
//                case OsmGeoType.Node:
//                    if (changesetNodes.TryGetValue(id, out var node))
//                    {
//                        return node;
//                    }
//                    break;
//                case OsmGeoType.Way:
//                    if (changesetWays.TryGetValue(id, out var way))
//                    {
//                        return way;
//                    }
//                    break;
//                case OsmGeoType.Relation:
//                    if (changesetRelations.TryGetValue(id, out var relation))
//                    {
//                        return relation;
//                    }
//                    break;
//            }
//            return baseSource.Get(type, id);
//        }
//    }
//}
