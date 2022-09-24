using LightningDB;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;
using OsmSharp.IO.Binary;

namespace OsmNightWatch
{
    public class OsmDatabaseWithReplicationData : IOsmGeoFilterableSource
    {
        private readonly IOsmGeoSource baseSource;

        private Dictionary<long, Relation?> changesetRelations = new();
        private Dictionary<long, Way?> changesetWays = new();
        private Dictionary<long, Node?> changesetNodes = new();

        public OsmDatabaseWithReplicationData(IOsmGeoSource baseSource, KeyValueDatabase keyValueDatabase)
        {
            this.baseSource = baseSource;
            //LoadChanges(keyValueDatabase);
        }

        private void LoadChanges(KeyValueDatabase keyValueDatabase)
        {
            using var tx = keyValueDatabase.BeginTransaction();
            using var dbNodes = tx.OpenDatabase("ChangesetsNodes", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            using var dbWays = tx.OpenDatabase("ChangesetsWays", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            using var dbRelations = tx.OpenDatabase("ChangesetsRelations", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            using var cursorNodes = tx.CreateCursor(dbNodes);
            using var cursorWays = tx.CreateCursor(dbWays);
            using var cursorRelations = tx.CreateCursor(dbRelations);
            foreach (var node in cursorNodes.AsEnumerable())
            {
                changesetNodes.Add(BitConverter.ToInt64(node.Item1.AsSpan()), node.Item2.AsSpan().Length == 0 ? null : Decode(node.Item2.AsSpan()) as Node);
            }
            foreach (var way in cursorWays.AsEnumerable())
            {
                changesetWays.Add(BitConverter.ToInt64(way.Item1.AsSpan()), way.Item2.AsSpan().Length == 0 ? null : Decode(way.Item2.AsSpan()) as Way);
            }
            foreach (var relation in cursorRelations.AsEnumerable())
            {
                changesetRelations.Add(BitConverter.ToInt64(relation.Item1.AsSpan()), relation.Item2.AsSpan().Length == 0 ? null : Decode(relation.Item2.AsSpan()) as Relation);
            }
        }

        private OsmGeo Decode(ReadOnlySpan<byte> readOnlySpan)
        {
            ms.Position = 0;
            ms.Write(readOnlySpan);
            ms.Position = 0;
            return BinarySerializer.ReadOsmGeo(ms);
        }

        public void ApplyChangeset(OsmChange changeset, LightningDB.LightningTransaction tx)
        {
            var dbNodes = tx.OpenDatabase("ChangesetsNodes", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            var dbWays = tx.OpenDatabase("ChangesetsWays", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            var dbRelations = tx.OpenDatabase("ChangesetsRelations", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            foreach (var change in changeset.Create)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        Put(tx, dbNodes, change);
                        break;
                    case OsmGeoType.Way:
                        Put(tx, dbWays, change);
                        break;
                    case OsmGeoType.Relation:
                        Put(tx, dbRelations, change);
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
            foreach (var change in changeset.Modify)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        Put(tx, dbNodes, change);
                        break;
                    case OsmGeoType.Way:
                        Put(tx, dbWays, change);
                        break;
                    case OsmGeoType.Relation:
                        Put(tx, dbRelations, change);
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
            foreach (var change in changeset.Delete)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                        {
                            //tx.Put(dbNodes, BitConverter.GetBytes(idNode), Array.Empty<byte>()).ThrowOnError();
                            changesetNodes[idNode] = null;
                        }
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                        {
                            //tx.Put(dbWays, BitConverter.GetBytes(idWay), Array.Empty<byte>()).ThrowOnError();
                            changesetWays[idWay] = null;
                        }
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                        {
                            //tx.Put(dbRelations, BitConverter.GetBytes(idRelation), Array.Empty<byte>()).ThrowOnError();
                            changesetRelations[idRelation] = null;
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        MemoryStream ms = new MemoryStream();

        private void Put(LightningTransaction tx, LightningDatabase db, OsmGeo element)
        {
            //switch (element.Type)
            //{
            //    case OsmGeoType.Node:
            //        changesetNodes[(long)element.Id!] = (Node)element;
            //        break;
            //    case OsmGeoType.Way:
            //        changesetWays[(long)element.Id!] = (Way)element;
            //        break;
            //    case OsmGeoType.Relation:
            //        changesetRelations[(long)element.Id!] = (Relation)element;
            //        break;
            //}

            //ms.Position = 0;
            //BinarySerializer.Append(ms, element);
            //tx.Put(db, BitConverter.GetBytes((long)element.Id!).AsSpan(), ms.ToArray().AsSpan(0, (int)ms.Position)).ThrowOnError();
        }

        public void BatchLoad(HashSet<long> nodeIds, HashSet<long> wayIds, HashSet<long> relationIds)
        {
            (baseSource as IOsmGeoBatchSource)?.BatchLoad(nodeIds, wayIds, relationIds);
        }

        Dictionary<FilterSettings, List<OsmGeo>> _cache = new();

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
                    _cache[filterSettings] = results = baseFilterableSource.Filter(filterSettings).ToList();
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
