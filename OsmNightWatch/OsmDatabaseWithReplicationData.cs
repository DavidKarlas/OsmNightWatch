using LightningDB;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;
using OsmSharp.IO.Binary;

namespace OsmNightWatch
{
    public class OsmDatabaseWithReplicationData : IOsmGeoBatchSource
    {
        private readonly IOsmGeoSource baseSource;

        public OsmDatabaseWithReplicationData(IOsmGeoSource baseSource)
        {
            this.baseSource = baseSource;
        }

        public void ApplyChangeset(OsmChange changeset, LightningDB.LightningTransaction tx)
        {
            using var dbNodes = tx.OpenDatabase("ChangesetsNodes", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            using var dbWays = tx.OpenDatabase("ChangesetsWays", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            using var dbRelations = tx.OpenDatabase("ChangesetsRelations", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            foreach (var change in changeset.Delete)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                            tx.Put(dbNodes, BitConverter.GetBytes(idNode), Array.Empty<byte>());
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                            tx.Put(dbWays, BitConverter.GetBytes(idWay), Array.Empty<byte>());
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                            tx.Put(dbRelations, BitConverter.GetBytes(idRelation), Array.Empty<byte>());
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
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
        }

        MemoryStream ms = new MemoryStream();

        private void Put(LightningTransaction tx, LightningDatabase db, OsmGeo element)
        {
            ms.Position = 0;
            BinarySerializer.Append(ms, element);
            tx.Put(db, BitConverter.GetBytes((long)element.Id!), ms.ToArray());
        }
        private (bool Found, OsmGeo Element) Get(LightningTransaction tx, LightningDatabase db, long elementId)
        {
            var (code, key, value) = tx.Get(db, BitConverter.GetBytes(elementId));
            if (code == MDBResultCode.Success)
            {
                ms.Position = 0;
                var span = value.AsSpan();
                ms.Write(span);
                return (true, BinarySerializer.ReadOsmGeo(ms));
            }
            return (false, null);
        }

        public void BatchLoad(HashSet<long> nodeIds, HashSet<long> wayIds, HashSet<long> relationIds)
        {
            (baseSource as IOsmGeoBatchSource)?.BatchLoad(nodeIds, wayIds, relationIds);
        }

        public OsmGeo Get(OsmGeoType type, long id)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    var (foundNode, node) = Get( id);
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
