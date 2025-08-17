using LightningDB;
using MonoTorrent.Client;
using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Prepared;
using NetTopologySuite.Index.Strtree;
using NetTopologySuite.IO;
using OsmNightWatch.Analyzers.AdminCountPerCountry;
using OsmNightWatch.PbfParsing;
using OsmSharp.Tags;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Reflection.PortableExecutable;

namespace OsmNightWatch
{
    public class KeyValueDatabase : IDisposable
    {
        private const int TimestampDbKey = 1;
        private readonly LightningEnvironment dbEnv;
        private LightningTransaction? transaction;
        private Dictionary<string, LightningDatabase> databases = new();
        public RelationChangesTracker RelationChangesTracker { get; }

        private static LightningEnvironment CreateEnv(string storePath)
        {
            var dbEnv = new LightningEnvironment(storePath);
            dbEnv.MaxDatabases = 10;
            dbEnv.MapSize = 128L * 1024L * 1024L * 1024L;//16GB should be enough, I hope
            dbEnv.Open();
            return dbEnv;
        }

        public KeyValueDatabase(string storePath)
        {
            dbEnv = CreateEnv(storePath);
            BeginTransaction();
            RelationChangesTracker = ReadRelationChangesTracker();
            AbortTransaction();
        }

        public void BeginTransaction()
        {
            if (transaction != null)
            {
                throw new InvalidOperationException("Transaction already started!");
            }
            transaction = dbEnv.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            WriteRelationChangesTracker(RelationChangesTracker);
            tx.Commit();
            tx.Dispose();
            foreach (var db in databases.Values)
            {
                db.Dispose();
            }
            databases.Clear();
            transaction = null;
        }

        public void AbortTransaction()
        {
            if (transaction is not LightningTransaction tx)
            {
                return;
            }
            tx.Dispose();
            foreach (var db in databases.Values)
            {
                db.Dispose();
            }
            databases.Clear();
            transaction = null;
        }

        public void SetTimestamp(DateTime timestamp)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Settings");
            tx.Put(db, BitConverter.GetBytes(TimestampDbKey), BitConverter.GetBytes(timestamp.ToBinary()));
        }

        private LightningDatabase OpenDb(LightningTransaction tx, string name)
        {
            if (!databases.TryGetValue(name, out var db))
            {
                databases[name] = db = tx.OpenDatabase(name, new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            }
            return db;
        }

        public DateTime? GetTimestamp()
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Settings");
            var timestampValue = tx.Get(db, BitConverter.GetBytes(TimestampDbKey));
            if (timestampValue.resultCode == MDBResultCode.Success)
            {
                return DateTime.FromBinary(BitConverter.ToInt64(timestampValue.value.AsSpan()));
            }
            return null;
        }
        public void UpdateNodes(Dictionary<long, Node?> changesetNodes, bool initial = false)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Nodes");
            Span<byte> keyBuffer = stackalloc byte[8];
            Span<byte> originalBuffer = stackalloc byte[32 * 1024];
            foreach (var element in (initial ? (IEnumerable<KeyValuePair<long, Node?>>)changesetNodes.OrderBy(e => e.Key) : changesetNodes))
            {
                BinaryPrimitives.WriteInt64BigEndian(keyBuffer, element.Key);
                if (element.Value == null)
                {
                    tx.Put(db, keyBuffer, ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    var buffer = originalBuffer;
                    BinSerialize.WriteDouble(ref buffer, element.Value.Latitude);
                    BinSerialize.WriteDouble(ref buffer, element.Value.Longitude);
                    WriteTags(ref buffer, element.Value.Tags);
                    buffer = originalBuffer.Slice(0, originalBuffer.Length - buffer.Length);
                    if (initial)
                    {
                        tx.Put(db, keyBuffer, buffer, PutOptions.AppendData);
                    }
                    else
                    {
                        tx.Put(db, keyBuffer, buffer);
                    }
                }
            }
        }

        public void WriteRelationChangesTracker(RelationChangesTracker tracker)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Tracker_NodeToWay");
            var emptyDb = db.DatabaseStats.Entries == 0;
            if (emptyDb)
            {
                Span<byte> keyBuffer = stackalloc byte[8];
                Span<byte> buffer = stackalloc byte[4 * 256];
                foreach (var ways in tracker.NodeToWay.OrderBy(n => n.Key))
                {
                    BinaryPrimitives.WriteInt64BigEndian(keyBuffer, ways.Key);
                    var span = buffer;
                    foreach (var wayId in ways.Value)
                    {
                        BinSerialize.WriteUInt(ref span, wayId);
                    }
                    tx.Put(db, keyBuffer, buffer[..^span.Length], PutOptions.AppendData);
                }
                db = OpenDb(tx, "Tracker_WayToRelation");
                keyBuffer = stackalloc byte[4];
                foreach (var relations in tracker.WayToRelation.OrderBy(w => w.Key))
                {
                    BinaryPrimitives.WriteUInt32BigEndian(keyBuffer, relations.Key);
                    var span = buffer;
                    foreach (var relationId in relations.Value)
                    {
                        BinSerialize.WriteUInt(ref span, relationId);
                    }
                    tx.Put(db, keyBuffer, buffer[..^span.Length]);
                }
                db = OpenDb(tx, "Tracker_NodeToRelation");
                keyBuffer = stackalloc byte[8];
                foreach (var relations in tracker.NodeToRelation.OrderBy(n => n.Key))
                {
                    BinaryPrimitives.WriteInt64BigEndian(keyBuffer, relations.Key);
                    var span = buffer;
                    foreach (var relationId in relations.Value)
                    {
                        BinSerialize.WriteUInt(ref span, relationId);
                    }
                    tx.Put(db, keyBuffer, buffer[..^span.Length]);
                }
                db = OpenDb(tx, "Tracker_Relations");
                keyBuffer = stackalloc byte[4];
                foreach (var relation in tracker.Relations.OrderBy(n => n))
                {
                    BinaryPrimitives.WriteUInt32BigEndian(keyBuffer, relation);
                    tx.Put(db, keyBuffer, keyBuffer, PutOptions.AppendData);
                }
            }
            else
            {
                Span<byte> keyBuffer = stackalloc byte[8];
                Span<byte> buffer = stackalloc byte[4 * 256];
                foreach (var ways in tracker.NodeToWay)
                {
                    BinaryPrimitives.WriteInt64BigEndian(keyBuffer, ways.Key);
                    GetNodeToWay(ways.Key, ways.Value);
                    var span = buffer;
                    foreach (var wayId in ways.Value)
                    {
                        BinSerialize.WriteUInt(ref span, wayId);
                    }
                    tx.Put(db, keyBuffer, buffer[..^span.Length]);
                }
                db = OpenDb(tx, "Tracker_WayToRelation");
                keyBuffer = stackalloc byte[4];
                foreach (var relations in tracker.WayToRelation)
                {
                    BinaryPrimitives.WriteUInt32BigEndian(keyBuffer, relations.Key);
                    GetWayToRelation(relations.Key, relations.Value);
                    var span = buffer;
                    foreach (var relationId in relations.Value)
                    {
                        BinSerialize.WriteUInt(ref span, relationId);
                    }
                    tx.Put(db, keyBuffer, buffer[..^span.Length]);
                }
                db = OpenDb(tx, "Tracker_NodeToRelation");
                keyBuffer = stackalloc byte[8];
                foreach (var relations in tracker.NodeToRelation)
                {
                    BinaryPrimitives.WriteInt64BigEndian(keyBuffer, relations.Key);
                    GetNodeToRelation(relations.Key, relations.Value);
                    var span = buffer;
                    foreach (var relationId in relations.Value)
                    {
                        BinSerialize.WriteUInt(ref span, relationId);
                    }
                    tx.Put(db, keyBuffer, buffer[..^span.Length]);
                }
                db = OpenDb(tx, "Tracker_Relations");
                keyBuffer = stackalloc byte[4];
                foreach (var relation in tracker.Relations)
                {
                    BinaryPrimitives.WriteUInt32BigEndian(keyBuffer, relation);
                    tx.Put(db, keyBuffer, keyBuffer);
                }
            }

            tracker.WayToRelation.Clear();
            tracker.NodeToWay.Clear();
            tracker.Relations.Clear();
        }

        public RelationChangesTracker ReadRelationChangesTracker()
        {
            var tracker = new RelationChangesTracker(this);

            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Tracker_Relations");
            Span<byte> keyBuffer = stackalloc byte[4];
            var cursor = tx.CreateCursor(db);
            foreach (var entry in cursor.AsEnumerable())
            {
                tracker.TrackedRelations.Add(BinaryPrimitives.ReadUInt32BigEndian(entry.Item1.AsSpan()));
            }
            return tracker;
        }

        private void WriteTags(ref Span<byte> span, TagsCollectionBase? tags)
        {
            if (tags == null)
            {
                return;
            }
            foreach (var tag in tags)
            {
                BinSerialize.WriteString(ref span, tag.Key);
                BinSerialize.WriteString(ref span, tag.Value);
            }
        }

        private TagsCollectionBase? ReadTags(ref ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length == 0)
            {
                return null;
            }
            var tags = new TagsCollection();
            while (buffer.Length > 0)
            {
                var key = BinSerialize.ReadString(ref buffer);
                var value = BinSerialize.ReadString(ref buffer);
                tags.Add(key, value);
            }
            return tags;
        }

        public void UpdateWays(Dictionary<long, Way?> changesetWays, bool initial = false)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Ways");
            Span<byte> keyBuffer = stackalloc byte[8];
            Span<byte> originalBuffer = stackalloc byte[128 * 1024];
            foreach (var element in changesetWays)
            {
                BinaryPrimitives.WriteInt64BigEndian(keyBuffer, element.Key);
                if (element.Value == null)
                {
                    tx.Put(db, keyBuffer, ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    var buffer = originalBuffer;
                    BinSerialize.WriteUShort(ref buffer, (ushort)element.Value.Nodes.Length);
                    foreach (var nodeId in element.Value.Nodes)
                    {
                        BinSerialize.WriteLong(ref buffer, nodeId);
                    }
                    WriteTags(ref buffer, element.Value.Tags);
                    tx.Put(db, keyBuffer, originalBuffer.Slice(0, originalBuffer.Length - buffer.Length));
                }
            }
        }

        public void UpdateRelations(Dictionary<long, Relation?> changesetRelations, bool initial = false)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }

            var db = OpenDb(tx, "Relations");
            Span<byte> keyBuffer = stackalloc byte[8];
            Span<byte> originalBuffer = stackalloc byte[1024 * 1024];
            foreach (var element in changesetRelations)
            {
                BinaryPrimitives.WriteInt64BigEndian(keyBuffer, element.Key);
                if (element.Value == null)
                {
                    tx.Put(db, keyBuffer, ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    var buffer = originalBuffer;
                    BinSerialize.WriteUShort(ref buffer, (ushort)element.Value.Members.Length);
                    foreach (var member in element.Value.Members)
                    {
                        BinSerialize.WriteLong(ref buffer, member.Id);
                        BinSerialize.WriteString(ref buffer, member.Role);
                        BinSerialize.WriteByte(ref buffer, (byte)member.Type);
                    }
                    WriteTags(ref buffer, element.Value.Tags);
                    tx.Put(db, keyBuffer, originalBuffer.Slice(0, originalBuffer.Length - buffer.Length));
                }
            }
        }

        public bool TryGetNode(long id, out Node? nodeFromDb)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Nodes");
            Span<byte> keyBuffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBuffer, id);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var buffer = read.value.AsSpan();
                if (buffer.Length == 0)
                {
                    nodeFromDb = null;
                    return true;
                }
                var lat = BinSerialize.ReadDouble(ref buffer);
                var lon = BinSerialize.ReadDouble(ref buffer);
                var tags = ReadTags(ref buffer);
                nodeFromDb = new Node(id, lat, lon, tags);
                return true;
            }
            nodeFromDb = null;
            return false;
        }

        public bool TryGetWay(long id, out Way? wayFromDb)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Ways");
            Span<byte> keyBuffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBuffer, id);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var buffer = read.value.AsSpan();
                if (buffer.Length == 0)
                {
                    wayFromDb = null;
                    return true;
                }
                int numberOfNodes = BinSerialize.ReadUShort(ref buffer);
                var nodes = new long[numberOfNodes];
                for (int i = 0; i < numberOfNodes; i++)
                {
                    nodes[i] = BinSerialize.ReadLong(ref buffer);
                }
                var tags = ReadTags(ref buffer);
                wayFromDb = new Way(id, nodes, tags);
                return true;
            }
            wayFromDb = null;
            return false;
        }
        public bool TryGetRelation(long id, out Relation? relationFromDb)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Relations");
            Span<byte> keyBuffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBuffer, id);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var buffer = read.value.AsSpan();
                if (buffer.Length == 0)
                {
                    relationFromDb = null;
                    return true;
                }
                int numberOfMembers = BinSerialize.ReadUShort(ref buffer);
                var members = new RelationMember[numberOfMembers];
                for (int i = 0; i < numberOfMembers; i++)
                {
                    var memberId = BinSerialize.ReadLong(ref buffer);
                    var role = BinSerialize.ReadString(ref buffer);
                    var type = (OsmGeoType)BinSerialize.ReadByte(ref buffer);
                    members[i] = new RelationMember(memberId, role, type);
                }
                var tags = ReadTags(ref buffer);
                relationFromDb = new Relation(id, members, tags);
                return true;
            }
            relationFromDb = null;
            return false;
        }

        public void Dispose()
        {
            dbEnv.Dispose();
        }

        public void GetNodeToRelation(long nodeId, HashSet<uint> relations)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Tracker_NodeToRelation");
            Span<byte> keyBuffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBuffer, nodeId);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var buffer = read.value.AsSpan();
                while (buffer.Length > 0)
                {
                    relations.Add(BinSerialize.ReadUInt(ref buffer));
                }
            }
        }

        public void GetNodeToWay(long nodeId, HashSet<uint> ways)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Tracker_NodeToWay");
            Span<byte> keyBuffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBuffer, nodeId);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var buffer = read.value.AsSpan();
                while (buffer.Length > 0)
                {
                    ways.Add(BinSerialize.ReadUInt(ref buffer));
                }
            }
        }

        public void GetWayToRelation(uint wayId, HashSet<uint> relations)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "Tracker_WayToRelation");
            Span<byte> keyBuffer = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(keyBuffer, wayId);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var buffer = read.value.AsSpan();
                while (buffer.Length > 0)
                {
                    relations.Add(BinSerialize.ReadUInt(ref buffer));
                }
            }
        }

        internal void StoreCoastline(Dictionary<long, Way?> coastlineWays)
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "CoastlineWays");
            Span<byte> keyBuffer = stackalloc byte[8];
            Span<byte> originalBuffer = stackalloc byte[128 * 1024];
            foreach (var element in coastlineWays)
            {
                BinaryPrimitives.WriteInt64BigEndian(keyBuffer, element.Key);
                if (element.Value == null)
                {
                    tx.Delete(db, keyBuffer);
                }
                else
                {
                    var buffer = originalBuffer;
                    BinSerialize.WriteUShort(ref buffer, (ushort)element.Value.Nodes.Length);
                    foreach (var nodeId in element.Value.Nodes)
                    {
                        BinSerialize.WriteLong(ref buffer, nodeId);
                    }
                    WriteTags(ref buffer, element.Value.Tags);
                    tx.Put(db, keyBuffer, originalBuffer.Slice(0, originalBuffer.Length - buffer.Length));
                }
            }
        }

        internal Dictionary<long, Way> LoadCoastline()
        {
            var coastlineWays = new Dictionary<long, Way>();
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, "CoastlineWays");
            Span<byte> keyBuffer = stackalloc byte[8];
            var cursor = tx.CreateCursor(db);
            foreach (var entry in cursor.AsEnumerable())
            {
                var key = BinaryPrimitives.ReadInt64BigEndian(entry.Item1.AsSpan());
                var buffer = entry.Item2.AsSpan();
                if (buffer.Length == 0)
                {
                    throw new Exception("How did lenght=0 end up in CoastlineWays?");
                }
                int numberOfNodes = BinSerialize.ReadUShort(ref buffer);
                var nodes = new long[numberOfNodes];
                for (int i = 0; i < numberOfNodes; i++)
                {
                    nodes[i] = BinSerialize.ReadLong(ref buffer);
                }
                var tags = ReadTags(ref buffer);
                coastlineWays.Add(key, new Way(key, nodes, tags));
            }
            return coastlineWays;
        }
    }
}
