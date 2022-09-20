using LightningDB;
using OsmNightWatch.Analyzers;
using OsmNightWatch.PbfParsing;
using OsmSharp;
using OsmSharp.Db;
using OsmSharp.Replication;
using System.Text;

namespace OsmNightWatch
{
    internal class ReplicationData
    {
        private readonly LightningEnvironment dbEnv;
        private readonly string pbfPath;
        private readonly PbfIndex pbfIndex;

        public ReplicationData(string pbfPath, PbfIndex pbfIndex, string storePath)
        {
            dbEnv = CreateEnv(storePath);
            ms = new MemoryStream(msBuffer);
            this.pbfPath = pbfPath;
            this.pbfIndex = pbfIndex;
        }

        public async Task InitializeAsync()
        {
            var seqNumber = GetSequenceNumber();
            if (seqNumber == null)
            {
                var offset = pbfIndex.GetLastNodeOffset();
                var lastNodesWithMeta = NodesParser.LoadNodesWithMetadata(pbfPath, offset).Last();
                if (lastNodesWithMeta.TimeStamp is not DateTime datetime)
                    throw new NotSupportedException();
                var enumerator = new CatchupReplicationDiffEnumerator(datetime);
                if (await enumerator.MoveNext())
                {
                    SetSequenceNumber(enumerator.State.SequenceNumber);
                }
                else
                {
                    throw new InvalidOperationException();//not sure how this could happen
                }
            }
        }

        private void SetSequenceNumber(long sequenceNumber)
        {
            using (var tx = dbEnv.BeginTransaction())
            using (var db = tx.OpenDatabase("ReplicationState", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create }))
            {
                tx.Put(db, Encoding.UTF8.GetBytes("sequenceNumber"), BitConverter.GetBytes(sequenceNumber));
            }
        }

        private long? GetSequenceNumber()
        {
            using (var tx = dbEnv.BeginTransaction())
            using (var db = tx.OpenDatabase("ReplicationState", new DatabaseConfiguration() { Flags = DatabaseOpenFlags.Create }))
            {
                var sequenceNumber = tx.Get(db, Encoding.UTF8.GetBytes("sequenceNumber"));
                if (sequenceNumber.resultCode == MDBResultCode.Success)
                {
                    return BitConverter.ToInt64(sequenceNumber.value.AsSpan());
                }
            }
            return null;
        }

        public async Task ProcessNext(IOsmGeoSource oldDb, IEnumerable<IOsmAnalyzer> analyzers)
        {
            var seqNumberN = GetSequenceNumber();
            if (seqNumberN is not long seqNumber)
            {
                throw new Exception($"Did you forget to call {nameof(InitializeAsync)}?");
            }
            seqNumber++;
            var changeset = await ReplicationConfig.Minutely.DownloadDiff(seqNumber);
            if (changeset is null)
            {
                throw new Exception($"No changeset found for sequence number {seqNumber}");
            }
            var newDb = new OsmDatabaseWithChangeset(oldDb, changeset);
            foreach (var analyzer in analyzers)
            {
                foreach (var relation in changeset.Delete.Union(changeset.Modify).Union(changeset.Create).OfType<Relation>())
                {
                    analyzer.AnalyzeRelation(relation, oldDb, newDb);
                }
            }
        }

        public static LightningEnvironment CreateEnv(string storePath)
        {
            var dbEnv = new LightningEnvironment(storePath);
            dbEnv.MaxDatabases = 10;
            dbEnv.MapSize = 4L * 1024L * 1024L * 1024L;
            dbEnv.Open();
            return dbEnv;
        }

        byte[] msBuffer = new byte[1024 * 1024];
        byte[] stringBuffer = new byte[12 * 1024];
        MemoryStream ms;

        public void ProcessNode(OsmSharp.IO.PBF.PrimitiveBlock block, OsmSharp.IO.PBF.Node node)
        {
            //BinarySerializer.Append(ms, OsmSharp.IO.PBF.Encoder.DecodeNode(block, node, reusableNode), stringBuffer);
            //Span<byte> idSpan = stackalloc byte[8];
            //BinaryPrimitives.WriteInt64BigEndian(idSpan, node.id);
            //tx.Put(db, idSpan, new Span<byte>(msBuffer, 0, (int)ms.Position), PutOptions.AppendData);
            //totalSize += ms.Position;
            //ms.Position = 0;
        }

        public void ProcessRelation(OsmSharp.IO.PBF.PrimitiveBlock block, OsmSharp.IO.PBF.Relation relation)
        {
            //if (firstRelation)
            //{
            //    firstWay = false;
            //    OpenDb(OsmGeoType.Relation);
            //}
            //BinarySerializer.Append(ms, OsmSharp.IO.PBF.Encoder.DecodeRelation(block, relation, reusableRelation), stringBuffer);
            //Span<byte> idSpan = stackalloc byte[8];
            //BinaryPrimitives.WriteInt64BigEndian(idSpan, relation.id);
            //tx.Put(db, idSpan, new Span<byte>(msBuffer, 0, (int)ms.Position), PutOptions.AppendData);
            //totalSize += ms.Position;
            //ms.Position = 0;
        }

        public void ProcessWay(OsmSharp.IO.PBF.PrimitiveBlock block, OsmSharp.IO.PBF.Way way)
        {
            //if (firstWay)
            //{
            //    firstWay = false;
            //    OpenDb(OsmGeoType.Way);
            //}
            //BinarySerializer.Append(ms, OsmSharp.IO.PBF.Encoder.DecodeWay(block, way, reusableWay), stringBuffer);
            //Span<byte> idSpan = stackalloc byte[8];
            //BinaryPrimitives.WriteInt64BigEndian(idSpan, way.id);
            //tx.Put(db, idSpan, new Span<byte>(msBuffer, 0, (int)ms.Position), PutOptions.AppendData);
            //totalSize += ms.Position;
            //ms.Position = 0;
        }


        public void Dispose()
        {
            //tx?.Commit();
            //db?.Dispose();
            //tx?.Dispose();
            //dbEnv?.Dispose();
        }
    }
}
