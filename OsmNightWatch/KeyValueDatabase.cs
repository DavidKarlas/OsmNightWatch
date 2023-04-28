using LightningDB;
using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Prepared;
using NetTopologySuite.Index.Strtree;
using Npgsql;
using OsmNightWatch.Analyzers.AdminCountPerCountry;
using OsmNightWatch.PbfParsing;
using OsmSharp.Tags;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace OsmNightWatch
{
    public class KeyValueDatabase : IDisposable
    {
        private const int TimestampDbKey = 1;
        private readonly LightningEnvironment dbEnv;
        NpgsqlDataSource dataSource;
        private LightningTransaction? transaction;
        private Dictionary<string, LightningDatabase> databases = new();

        private static LightningEnvironment CreateEnv(string storePath)
        {
            var dbEnv = new LightningEnvironment(storePath);
            dbEnv.MaxDatabases = 10;
            dbEnv.MapSize = 16L * 1024L * 1024L * 1024L;//16GB should be enough, I hope
            dbEnv.Open();
            return dbEnv;
        }

        public KeyValueDatabase(string storePath)
        {
            dbEnv = CreateEnv(storePath);
            var dataSourceBuilder = new NpgsqlDataSourceBuilder("Host=localhost;Username=postgres;Password=a;Database=postgis_33_sample");
            dataSourceBuilder.UseNetTopologySuite();
            dataSource = dataSourceBuilder.Build();
        }

        public void Initialize()
        {
            using (var comm = dataSource.CreateCommand("CREATE TABLE IF NOT EXISTS Admins (Id int PRIMARY KEY, FriendlyName text, AdminLevel int, geom GEOMETRY)"))
                comm.ExecuteNonQuery();
            using (var comm = dataSource.CreateCommand("CREATE INDEX IF NOT EXISTS admin_geom_index  ON admins  USING GIST (geom);"))
                comm.ExecuteNonQuery();
            //using (var comm = dataSource.CreateCommand("CREATE INDEX admin_level_index  ON admins  USING HASH (AdminLevel);"))
            //    comm.ExecuteNonQuery();
        }

        public void DeleteAdmin(long id)
        {
            using (var comm = dataSource.CreateCommand("DELETE FROM Admins WHERE Id = @id"))
            {
                comm.Parameters.AddWithValue("id", id);
                comm.ExecuteNonQuery();
            }
        }

        public void UpsertAdmin(long id, string friendlyName, int adminLevel, Geometry? polygon)
        {
            using (var comm = dataSource.CreateCommand("INSERT INTO Admins (Id, FriendlyName, AdminLevel, geom) VALUES (@id, @friendlyName, @adminLevel, @geom) ON CONFLICT (Id) DO UPDATE SET geom = @geom, FriendlyName = @friendlyName, AdminLevel = @adminLevel"))
            {
                comm.Parameters.AddWithValue("id", id);
                comm.Parameters.AddWithValue("friendlyName", friendlyName);
                comm.Parameters.AddWithValue("adminLevel", adminLevel);
                comm.Parameters.AddWithValue("geom", NpgsqlTypes.NpgsqlDbType.Geometry, polygon ?? (object)DBNull.Value);
                comm.ExecuteNonQuery();
            }
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
                throw new InvalidOperationException("Transaction not started!");
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
            Span<byte> originalBuffer = stackalloc byte[16 * 1024];
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
                    var nodeId = ways.Key;
                    MemoryMarshal.Write(keyBuffer, ref nodeId);
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
                    var wayId = relations.Key;
                    MemoryMarshal.Write(keyBuffer, ref wayId);
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
                    var relationId = relation;
                    MemoryMarshal.Write(keyBuffer, ref relationId);
                    tx.Put(db, keyBuffer, keyBuffer, PutOptions.AppendData);
                }
            }
            else
            {
                Span<byte> keyBuffer = stackalloc byte[8];
                Span<byte> buffer = stackalloc byte[4 * 256];
                foreach (var ways in tracker.NodeToWay)
                {
                    var nodeId = ways.Key;
                    MemoryMarshal.Write(keyBuffer, ref nodeId);
                    GetNodeToWay(nodeId, ways.Value);
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
                    var wayId = relations.Key;
                    MemoryMarshal.Write(keyBuffer, ref wayId);
                    GetWayToRelation(wayId, relations.Value);
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
                    var relationId = relation;
                    MemoryMarshal.Write(keyBuffer, ref relationId);
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
                tracker.PersistentRelations.Add(MemoryMarshal.Read<uint>(entry.Item1.AsSpan()));
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
            dataSource.Dispose();
        }

        public List<long> GetCountryAdmins(long relationId, int adminLevel)
        {
            using (var comm = dataSource.CreateCommand("SELECT adm.id as id FROM admins adm INNER JOIN admins country ON (country.geom && adm.geom AND ST_Relate(country.geom, adm.geom, '2********')) WHERE country.id=@relationId and adm.adminlevel = @adminLevel;"))
            {
                comm.CommandTimeout = 120;
                comm.Parameters.AddWithValue("@relationId", relationId);
                comm.Parameters.AddWithValue("@adminLevel", adminLevel);
                using var reader = comm.ExecuteReader();
                {
                    var result = new List<long>();
                    while (reader.Read())
                    {
                        result.Add(reader.GetInt64(0));
                    }
                    return result;
                }
            }
        }


        public List<(long CountryId, int adminLevel)> GetCountryAndLevelForAdmins(List<uint> relevantAdmins)
        {
            if (relevantAdmins.Count == 0)
                return new List<(long, int)>();
            using (var comm = dataSource.CreateCommand($@"
SELECT country.id, adm.adminlevel
FROM admins adm
INNER JOIN admins country ON 
   (country.geom && adm.geom AND ST_Relate(country.geom, adm.geom, '2********'))
WHERE country.adminlevel=2 and adm.id in ({string.Join(",", relevantAdmins)})"))
            {
                comm.CommandTimeout = 120;
                using var reader = comm.ExecuteReader();
                {
                    var result = new List<(long CountryId, int adminLevel)>();
                    while (reader.Read())
                    {
                        result.Add((reader.GetInt64(0), reader.GetInt32(1)));
                    }
                    return result;
                }
            }
        }

        internal void Playground()
        {
            var sw3 = Stopwatch.StartNew();
            var countries = new Dictionary<long, PreparedPolygon>();
            var dict = new Dictionary<long, int>();
            var strs = new Dictionary<int, STRtree<long>>();
            using (var comm = dataSource.CreateCommand("SELECT id, adminlevel, geom AS Envelope from admins"))
            {
                using (var reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt64(0);
                        var adminLevel = reader.GetInt32(1);
                        var envelope = (Geometry)reader.GetValue(2);
                        dict.Add(id, adminLevel);
                        if (!strs.TryGetValue(adminLevel, out var str))
                            strs[adminLevel] = str = new();
                        PreparedPolygon e2 = null;
                        if (adminLevel == 2)
                        {
                            e2 = new PreparedPolygon((IPolygonal)envelope);
                            countries.Add(id, e2);
                        }
                        str.Insert(envelope.EnvelopeInternal, id);
                    }
                }
            }
            Console.WriteLine(sw3.Elapsed);
            foreach (var str in strs)
            {
                var sw = Stopwatch.StartNew();
                str.Value.Build();
                sw.Stop();
                Console.WriteLine(str.Key + " " + sw.Elapsed);
            }
            Console.WriteLine(dict.Count);
            Console.WriteLine(GC.GetTotalMemory(true) / (1024.0 * 1024));
            sw3.Restart();
            var candidates = new List<long>();
            var country = countries[365331];
            foreach (var candidate in strs[6].Query(country.Geometry.EnvelopeInternal))
            {
                candidates.Add(candidate);
            }
            Console.WriteLine(sw3.Elapsed);
            for (int i = 0; i < 5; i++)
            {
                sw3.Restart();
                var result = new ConcurrentBag<long>();
                var polies = new List<(long Id, Geometry Poly)>();
                using (var comm = dataSource.CreateCommand("SELECT id, geom from admins where id in (" + string.Join(",", candidates) + ")"))
                {
                    using (var reader = comm.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id = reader.GetInt64(0);
                            var geom = (Geometry)reader.GetValue(1);
                            polies.Add((id, geom));
                        }
                    }
                }
                sw3.Stop();
                Console.WriteLine(i + " " + sw3.Elapsed);
                Console.WriteLine(polies.Count);
                sw3.Restart();
                var result2 = Parallel.ForEach(polies, new ParallelOptions() { MaxDegreeOfParallelism = 24 }, (p) => {
                    if (country.Overlaps(p.Poly))
                        result.Add(p.Id);
                });
                sw3.Stop();
                Console.WriteLine(i + " " + sw3.Elapsed);
                Console.WriteLine(result.Count);
            }
            Console.WriteLine(GC.GetTotalMemory(true) / (1024.0 * 1024));
        }

        public bool DoesCountryExist(long relationId)
        {
            using (var comm = dataSource.CreateCommand("SELECT id FROM admins WHERE id=@relationId AND adminlevel=2 and geom IS NOT NULL"))
            {
                comm.Parameters.AddWithValue("@relationId", relationId);
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        return true;
                    }
                    return false;
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
            MemoryMarshal.Write(keyBuffer, ref nodeId);

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
            MemoryMarshal.Write(keyBuffer, ref wayId);

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

        public IEnumerable<(long RelationId, string name, int adminLevel)> GetBrokenAdmins()
        {

            using (var comm = dataSource.CreateCommand("SELECT id, friendlyname, adminlevel FROM admins WHERE geom IS NULL"))
            {
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        yield return (reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2));
                    }
                }
            }
        }
    }
}
