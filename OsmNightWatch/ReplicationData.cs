//using LightningDB;
//using OsmSharp;
//using OsmSharp.IO.Binary;
//using System;
//using System.Buffers.Binary;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace OsmNightWatch
//{
//    internal class ReplicationData
//    {
//        private readonly LightningEnvironment dbEnv;
//        private readonly string pbfPath;
//        private readonly PbfIndex pbfIndex;

//        public ReplicationData(string pbfPath, PbfIndex pbfIndex, string storePath)
//        {
//            dbEnv = CreateEnv(storePath);
//            ms = new MemoryStream(msBuffer);
//            this.pbfPath = pbfPath;
//            this.pbfIndex = pbfIndex;
//        }

//        public Task InitializeAsync()
//        {
//            var seqNumber = GetSequenceNumber();
//            if(seqNumber == null)
//            {

//            }
//        }

//        long? GetSequenceNumber()
//        {
//            using (var tx = dbEnv.BeginTransaction())
//            using (var db = tx.OpenDatabase("ReplicationState"))
//            {
//                var sequenceNumber = tx.Get(db, Encoding.UTF8.GetBytes("sequenceNumber"));
//                if (sequenceNumber.resultCode == MDBResultCode.Success)
//                {
//                    return BinaryPrimitives.ReadInt64BigEndian(sequenceNumber.value.AsSpan());
//                }
//            }
//            return null;
//        }

//        public static LightningEnvironment CreateEnv(string storePath)
//        {
//            var dbEnv = new LightningEnvironment(storePath);
//            dbEnv.MaxDatabases = 10;
//            dbEnv.MapSize = 4L * 1024L * 1024L * 1024L;
//            dbEnv.Open();
//            return dbEnv;
//        }

//        byte[] msBuffer = new byte[1024 * 1024];
//        byte[] stringBuffer = new byte[12 * 1024];
//        MemoryStream ms;

//        public void ProcessNode(OsmSharp.IO.PBF.PrimitiveBlock block, OsmSharp.IO.PBF.Node node)
//        {
//            BinarySerializer.Append(ms, OsmSharp.IO.PBF.Encoder.DecodeNode(block, node, reusableNode), stringBuffer);
//            Span<byte> idSpan = stackalloc byte[8];
//            BinaryPrimitives.WriteInt64BigEndian(idSpan, node.id);
//            tx.Put(db, idSpan, new Span<byte>(msBuffer, 0, (int)ms.Position), PutOptions.AppendData);
//            totalSize += ms.Position;
//            ms.Position = 0;

//            ProcessStats(OsmGeoType.Node, node.id);
//        }

//        public void ProcessRelation(OsmSharp.IO.PBF.PrimitiveBlock block, OsmSharp.IO.PBF.Relation relation)
//        {
//            if (firstWay)
//            {
//                firstWay = false;
//                OpenDb(OsmGeoType.Way);
//            }
//            BinarySerializer.Append(ms, OsmSharp.IO.PBF.Encoder.DecodeRelation(block, relation, reusableRelation), stringBuffer);
//            Span<byte> idSpan = stackalloc byte[8];
//            BinaryPrimitives.WriteInt64BigEndian(idSpan, relation.id);
//            tx.Put(db, idSpan, new Span<byte>(msBuffer, 0, (int)ms.Position), PutOptions.AppendData);
//            totalSize += ms.Position;
//            ms.Position = 0;

//            ProcessStats(OsmGeoType.Relation, relation.id);
//        }

//        public void ProcessWay(OsmSharp.IO.PBF.PrimitiveBlock block, OsmSharp.IO.PBF.Way way)
//        {
//            if (firstWay)
//            {
//                firstWay = false;
//                OpenDb(OsmGeoType.Way);
//            }
//            BinarySerializer.Append(ms, OsmSharp.IO.PBF.Encoder.DecodeWay(block, way, reusableWay), stringBuffer);
//            Span<byte> idSpan = stackalloc byte[8];
//            BinaryPrimitives.WriteInt64BigEndian(idSpan, way.id);
//            tx.Put(db, idSpan, new Span<byte>(msBuffer, 0, (int)ms.Position), PutOptions.AppendData);
//            totalSize += ms.Position;
//            ms.Position = 0;

//            ProcessStats(OsmGeoType.Way, way.id);
//        }

//        public void Process()
//        {
//            var reusableNode = new OsmSharp.IO.PBF.Node()
//            {
//                info = new OsmSharp.IO.PBF.Info()
//            };
//            OsmSharp.IO.PBF.PrimitiveBlock block;
//            while ((block = pbfReader.MoveNext()) != null)
//            {
//                OsmSharp.IO.PBF.Encoder.Decode(block, this, false, false, false, out _, out _, out _, reusableNode);
//            }
//        }

//        public void Dispose()
//        {
//            tx?.Commit();
//            db?.Dispose();
//            tx?.Dispose();
//            dbEnv?.Dispose();
//        }
//    }
//}
