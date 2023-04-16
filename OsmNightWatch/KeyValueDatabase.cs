using FASTER.core;
using LightningDB;
using OsmSharp;
using OsmSharp.IO.Binary;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace OsmNightWatch
{
    public class KeyValueDatabase : IDisposable
    {
        private const int TimestampDbKey = 1;
        private readonly LightningEnvironment dbEnv;
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
            if(!databases.TryGetValue(name, out var db))
            {
                databases[name] = db = tx.OpenDatabase(name, new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            }
            return db;
        }

        public DateTime? GetTimestamp()
        {
            using var tx = dbEnv.BeginTransaction();
            using var db = tx.OpenDatabase("Settings", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create });
            var timestampValue = tx.Get(db, BitConverter.GetBytes(TimestampDbKey));
            if (timestampValue.resultCode == MDBResultCode.Success)
            {
                return DateTime.FromBinary(BitConverter.ToInt64(timestampValue.value.AsSpan()));
            }
            return null;
        }
        public void UpdateNodes(Dictionary<long, Node?> changesetNodes)
        {
            UpdateValues("Nodes", changesetNodes);
        }

        public void UpdateWays(Dictionary<long, Way?> changesetWays)
        {
            UpdateValues("Ways", changesetWays);
        }

        public void UpdateRelations(Dictionary<long, Relation?> changesetRelations)
        {
            UpdateValues("Relations", changesetRelations);
        }

        private void UpdateValues<T>(string dbName, Dictionary<long, T?> elements) where T : OsmGeo
        {
            if (transaction is not LightningTransaction tx)
            {
                throw new InvalidOperationException("Transaction not started!");
            }
            var db = OpenDb(tx, dbName);
            var memory = new MemoryStream(64 * 1024);
            var buffer = new byte[8 * 1024];
            Span<byte> keyBuffer = stackalloc byte[8];
            foreach (var element in elements)
            {
                BinaryPrimitives.WriteInt64LittleEndian(keyBuffer, element.Key);
                if (element.Value == null)
                {
                    tx.Put(db, keyBuffer, ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    memory.SetLength(0);
                    BinarySerializer.Append(memory, element.Value, buffer);
                    tx.Put(db, keyBuffer, memory.ToArray());
                }
            }
        }

        public bool TryGetElement(string dbName, long id, out OsmGeo? element)
        {
            using var tx = dbEnv.BeginTransaction();
            var db = OpenDb(tx, dbName);
            Span<byte> keyBuffer = stackalloc byte[8];
            var buffer = new byte[8 * 1024];
            BinaryPrimitives.WriteInt64LittleEndian(keyBuffer, id);

            var read = tx.Get(db, keyBuffer);
            if (read.resultCode == MDBResultCode.Success)
            {
                var memory = new MemoryStream(read.value.CopyToNewArray());
                if (memory.Length == 0)
                {
                    element = null;
                    return true;
                }
                element = BinarySerializer.ReadOsmGeo(memory, buffer);
                return true;
            }
            element = null;
            return false;
        }

        public bool TryGetNode(long id, out Node? nodeFromDb)
        {
            var result = TryGetElement("Nodes", id, out var element);
            nodeFromDb = element as Node;
            return result;
        }
        public bool TryGetWay(long id, out Way? wayFromDb)
        {
            var result = TryGetElement("Ways", id, out var element);
            wayFromDb = element as Way;
            return result;
        }
        public bool TryGetRelation(long id, out Relation? relationFromDb)
        {
            var result = TryGetElement("Relations", id, out var element);
            relationFromDb = element as Relation;
            return result;
        }

        public void Dispose()
        {
            dbEnv.Dispose();
        }
    }
}
