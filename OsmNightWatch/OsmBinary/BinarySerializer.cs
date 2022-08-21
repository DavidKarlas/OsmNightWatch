// The MIT License (MIT)

// Copyright (c) 2017 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

using OsmSharp.Tags;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using OsmSharp.Db;

namespace OsmSharp.IO.Binary
{
    /// <summary>
    /// Contains all binary formatting code.
    /// </summary>
    public static class BinarySerializer
    {
        /// <summary>
        /// Appends the header byte(s).
        /// </summary>
        private static void AppendHeader(this Stream stream, OsmGeo osmGeo)
        {
            // build header containing type and nullable flags.
            byte header = 1; // a node.
            if(osmGeo.Type == OsmGeoType.Way)
            {
                header = 2;
            }
            else if(osmGeo.Type == OsmGeoType.Relation)
            {
                header = 3;
            }
            if (!osmGeo.Id.HasValue) { header = (byte)(header | 4); }
            if (!osmGeo.ChangeSetId.HasValue) { header = (byte)(header | 8); }
            if (!osmGeo.TimeStamp.HasValue) { header = (byte)(header | 16); }
            if (!osmGeo.UserId.HasValue) { header = (byte)(header | 32); }
            if (!osmGeo.Version.HasValue) { header = (byte)(header | 64); }
            if (!osmGeo.Visible.HasValue) { header = (byte)(header | 128); }
            stream.WriteByte(header);
        }
        
        /// <summary>
        /// Writes the given node starting at the stream's current position.
        /// </summary>
        public static void Append(this Stream stream, Node node, byte[] buffer = null)
        {
            if (node == null) { throw new ArgumentNullException(nameof(node)); }

            // appends the header.
            stream.AppendHeader(node);

            // write osm geo data.
            stream.AppendOsmGeo(node, buffer);

            // write lat/lon with nullable flags.
            stream.WriteVarInt64Nullable(node.Latitude.EncodeLatitude());
            stream.WriteVarInt64Nullable(node.Longitude.EncodeLongitude());
        }

        /// <summary>
        /// Writes the given way starting at the stream's current position.
        /// </summary>
        public static void Append(this Stream stream, Way way, byte[] buffer = null)
        {
            if (way == null) { throw new ArgumentNullException(nameof(way)); }

            // appends the header.
            stream.AppendHeader(way);

            // write data.
            stream.AppendOsmGeo(way, buffer);
            
            if (way.Nodes == null ||
                way.Nodes.Length == 0)
            {
                stream.WriteVarInt32(0);
            }
            else
            {
                stream.WriteVarInt32(way.Nodes.Length);
                stream.WriteVarInt64(way.Nodes[0]);
                for (var i = 1; i < way.Nodes.Length; i++)
                {
                    stream.WriteVarInt64(way.Nodes[i] - way.Nodes[i - 1]);
                }
            }
        }

        /// <summary>
        /// Writes the given relation starting at the stream's current position.
        /// </summary>
        public static void Append(this Stream stream, Relation relation, byte[] buffer = null)
        {
            if (relation == null) { throw new ArgumentNullException(nameof(relation)); }

            // appends the header.
            stream.AppendHeader(relation);

            // write data.
            stream.AppendOsmGeo(relation, buffer);
            
            if (relation.Members == null ||
                relation.Members.Length == 0)
            {
                stream.WriteVarInt32(0);
            }
            else
            {
                stream.WriteVarInt32(relation.Members.Length);
                stream.WriteVarInt64(relation.Members[0].Id);
                stream.WriteWithSize(relation.Members[0].Role, buffer);
                stream.WriteOsmGeoType(relation.Members[0].Type);
                
                for (var i = 1; i < relation.Members.Length; i++)
                {
                    stream.WriteVarInt64(relation.Members[i].Id - relation.Members[i - 1].Id);
                    stream.WriteWithSize(relation.Members[i].Role, buffer);
                    stream.WriteOsmGeoType(relation.Members[i].Type);
                }
            }
        }

        /// <summary>
        /// Writes the given osm geo object starting at the stream's current position.
        /// </summary>
        public static void Append(this Stream stream, OsmGeo osmGeo, byte[] buffer = null)
        {
            if (osmGeo is Node node)
            {
                stream.Append(node, buffer);
            }
            else if (osmGeo is Way way)
            {
                stream.Append(way, buffer);
            }
            else if (osmGeo is Relation relation)
            {
                stream.Append(relation, buffer);
            }
        }
        
        private static void AppendOsmGeo(this Stream stream, OsmGeo osmGeo, byte[] buffer)
        {
            if (osmGeo.Id.HasValue) { stream.WriteVarInt64(osmGeo.Id.Value); }
            if (osmGeo.Version.HasValue) { stream.WriteVarInt32((int)osmGeo.Version.Value); }
            if (osmGeo.ChangeSetId.HasValue) { stream.WriteVarInt64(osmGeo.ChangeSetId.Value); }
            if (osmGeo.TimeStamp.HasValue) { stream.WriteVarInt64(osmGeo.TimeStamp.Value.ToUnixTime()); }
            if (osmGeo.UserId.HasValue) { stream.WriteVarInt64(osmGeo.UserId.Value); }
            stream.WriteWithSize(osmGeo.UserName, buffer);
            if (osmGeo.Visible.HasValue) { stream.Write(osmGeo.Visible.Value); }
            
            if (osmGeo.Tags == null ||
                osmGeo.Tags.Count == 0)
            {
                stream.WriteVarInt32(0);
            }
            else
            {
                stream.WriteVarInt32(osmGeo.Tags.Count);
                foreach (var t in osmGeo.Tags)
                {
                    stream.WriteWithSize(t.Key, buffer);
                    stream.WriteWithSize(t.Value, buffer);
                }
            }
        }

        /// <summary>
        /// Reads the header, returns the type, and outputs the flags.
        /// </summary>
        internal static bool TryReadOsmGeoHeader(this Stream stream, out OsmGeoType type, out bool hasId, out bool hasChangesetId, out bool hasTimestamp,
            out bool hasUserId, out bool hasVersion, out bool hasVisible)
        {
            var header = stream.ReadByte();
            if (header == -1)
            {
                hasId = false;
                hasVersion = false;
                hasChangesetId = false;
                hasTimestamp = false;
                hasUserId = false;
                hasVersion = false;
                hasVisible = false;
                type = OsmGeoType.Node;
                return false;
            }

            hasId = (header & 4) == 0;
            hasChangesetId = (header & 8) == 0;
            hasTimestamp = (header & 16) == 0;
            hasUserId = (header & 32) == 0;
            hasVersion = (header & 64) == 0;
            hasVisible = (header & 128) == 0;

            var typeNumber = header & 3;            
            switch (typeNumber)
            {
                case 1:
                    type = OsmGeoType.Node;
                    break;
                case 2:
                    type = OsmGeoType.Way;
                    break;
                case 3:
                    type = OsmGeoType.Relation;
                    break;
                default:
                    throw new Exception("Invalid header: cannot detect OsmGeoType.");
            }

            return true;
        }

        /// <summary>
        /// Reads only the OSM type and id for the current object.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The id and type.</returns>
        public static (OsmGeoType type, long? id) ReadOsmGeoKey(this Stream stream)
        {            
            if (!stream.TryReadOsmGeoHeader(out var type, out var hasId, out var _, out var _,
                out var _, out var _, out var _)) throw new InvalidDataException("Could not read header.");

            // read the basics.
            if (!hasId) return (type, null);
            
            var id = stream.ReadVarInt64();
            return (type, id);
        }

        /// <summary>
        /// Skip over the next osm geo object without reading it.
        /// </summary>
        /// <param name="stream">The stream.</param>
        public static void SkipOsmGeo(this Stream stream)
        {
            if (!stream.TryReadOsmGeoHeader(out var type, out var hasId, out var hasChangesetId, out var hasTimestamp,
                out var hasUserId, out var hasVersion, out var hasVisible)) return; // couldn't read header.
            
            // read the basics.
            if (hasId) { stream.ReadVarInt64(); }
            if (hasVersion) { stream.ReadVarInt32(); }
            if (hasChangesetId) { stream.ReadVarInt64(); }
            if (hasTimestamp) { stream.ReadVarInt64().FromUnixTime(); }
            if (hasUserId) { stream.ReadVarInt64(); }
            stream.SkipStringWithSize();
            if (hasVisible) { stream.ReadBool(); }

            // read tags.
            var tagsCount = stream.ReadVarInt32();
            if (tagsCount > 0)
            {
                for (var i = 0; i < tagsCount; i++)
                {
                    stream.SkipStringWithSize();
                    stream.SkipStringWithSize();
                }
            }

            switch (type)
            {
                case OsmGeoType.Node:
                    stream.SkipNode();
                    break;
                case OsmGeoType.Way:
                     stream.SkipWay();
                    break;
                default:
                    stream.SkipRelation();
                    break;
            }
        }
        
        /// <summary>
        /// Reads an OSM object starting at the stream's current position.
        /// </summary>
        public static OsmGeo ReadOsmGeo(this Stream stream, byte[] buffer = null)
        {
            if (!stream.TryReadOsmGeoHeader(out var type, out var hasId, out var hasChangesetId, out var hasTimestamp,
                out var hasUserId, out var hasVersion, out var hasVisible)) return null; // couldn't read header.
            
            buffer ??= new byte [1024];
            if (buffer.Length < 1024) throw new ArgumentException("Buffer needs be at least 1024 bytes.", nameof(buffer));

            // read the basics.
            long? id = null;
            if (hasId) { id = stream.ReadVarInt64(); }
            int? version = null;
            if (hasVersion) { version = stream.ReadVarInt32(); }
            long? changesetId = null;
            if (hasChangesetId) { changesetId = stream.ReadVarInt64(); }
            DateTime? timestamp = null;
            if (hasTimestamp) { timestamp = stream.ReadVarInt64().FromUnixTime(); }
            long? userId = null;
            if (hasUserId) { userId = stream.ReadVarInt64(); }
            var username = stream.ReadWithSizeString(buffer);
            bool? visible = null;
            if (hasVisible) { visible = stream.ReadBool(); }

            // read tags.
            var tagsCount = stream.ReadVarInt32();
            TagsCollection tags = null;
            if (tagsCount > 0)
            {
                tags = new TagsCollection(tagsCount);
                for (var i = 0; i < tagsCount; i++)
                {
                    var key = stream.ReadWithSizeString(buffer);
                    var value = stream.ReadWithSizeString(buffer);
                    tags.AddOrReplace(key, value);
                }
            }

            OsmGeo osmGeo;
            switch (type)
            {
                case OsmGeoType.Node:
                    osmGeo = stream.ReadNode();
                    break;
                case OsmGeoType.Way:
                    osmGeo = stream.ReadWay();
                    break;
                default:
                    osmGeo = stream.ReadRelation(buffer);
                    break;
            }

            osmGeo.Id = id;
            osmGeo.ChangeSetId = changesetId;
            osmGeo.TimeStamp = timestamp;
            osmGeo.UserId = userId;
            osmGeo.UserName = username;
            osmGeo.Version = version;
            osmGeo.Visible = visible;
            osmGeo.Tags = tags;

            return osmGeo;
        }

        private static Node ReadNode(this Stream stream)
        {
            var node = new Node();
    
            node.Latitude = stream.ReadVarInt64Nullable().DecodeLatitude();
            node.Longitude = stream.ReadVarInt64Nullable().DecodeLongitude();

            return node;
        }

        private static void SkipNode(this Stream stream)
        {
            stream.ReadVarInt64Nullable();
            stream.ReadVarInt64Nullable();
        }

        private static Way ReadWay(this Stream stream)
        {
            var way = new Way();

            var nodeCount = stream.ReadVarInt32();
            if (nodeCount <= 0) return way;
            
            var nodes = new long[nodeCount];
            var node = stream.ReadVarInt64();
            nodes[0] = node;
            for (var i = 1; i < nodeCount; i++)
            {
                var nodeDif = stream.ReadVarInt64();
                node += nodeDif;
                nodes[i] = node;
            }
            way.Nodes = nodes;

            return way;
        }

        private static void SkipWay(this Stream stream)
        {
            var nodeCount = stream.ReadVarInt32();
            if (nodeCount <= 0) return;
            
            stream.ReadVarInt64();
            for (var i = 1; i < nodeCount; i++)
            {
                stream.ReadVarInt64();
            }
        }

        private static Relation ReadRelation(this Stream stream, byte[] buffer)
        {
            var relation = new Relation();
            
            var memberCount = stream.ReadVarInt32();
            if (memberCount <= 0) return relation;
            
            var members = new RelationMember[memberCount];
            var id = stream.ReadVarInt64();
            var role = stream.ReadWithSizeString(buffer);
            var type = stream.ReadOsmGeoType();
            members[0] = new RelationMember()
            {
                Id = id,
                Role = role,
                Type = type
            };
                
            for(var i = 1; i< memberCount; i++)
            {
                var idDif = stream.ReadVarInt64();
                id += idDif;
                role = stream.ReadWithSizeString(buffer);
                type = stream.ReadOsmGeoType();
                    
                members[i] = new RelationMember()
                {
                    Id = id,
                    Role = role,
                    Type = type
                };
            }
            relation.Members = members;

            return relation;
        }

        private static void SkipRelation(this Stream stream)
        {
            var memberCount = stream.ReadVarInt32();
            if (memberCount <= 0) return;
            
            stream.ReadVarInt64();
            stream.SkipStringWithSize();
            stream.ReadOsmGeoType();
                
            for(var i = 1; i< memberCount; i++)
            {
                stream.ReadVarInt64();
                stream.SkipStringWithSize();
                stream.ReadOsmGeoType();
            }
        }

        private static void WriteOsmGeoType(this Stream stream, OsmGeoType type)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    stream.WriteByte(1);
                    return;
                case OsmGeoType.Way:
                    stream.WriteByte(2);
                    break;
                case OsmGeoType.Relation:
                    stream.WriteByte(3);
                    break;
            }
        }

        private static OsmGeoType ReadOsmGeoType(this Stream stream)
        {
            var typeId = stream.ReadByte();
            var type = OsmGeoType.Node;
            switch (typeId)
            {
                case 2:
                    type = OsmGeoType.Way;
                    break;
                case 3:
                    type = OsmGeoType.Relation;
                    break;
            }

            return type;
        }

        private static long? EncodeLatitude(this double? latitude)
        {
            if (latitude == null) return null;
            
            return (long) (100000000 * latitude.Value);
        }

        private static double? DecodeLatitude(this long? encoded)
        {
            if (encoded == null) return null;

            return (encoded.Value / 100000000.0);
        }

        private static long? EncodeLongitude(this double? latitude)
        {
            if (latitude == null) return null;
            
            return (long) (1000000000 * latitude);
        }

        private static double? DecodeLongitude(this long? encoded)
        {
            if (encoded == null) return null;

            return (encoded / 1000000000.0);
        }
//
//        private static int WriteDouble(this Stream stream, double value)
//        {
//            BitCoder.WriteInt64(stream, BitConverter.DoubleToInt64Bits(value));
//            //stream.Write(BitConverter.GetBytes(value), 0, 8);
//            return 8;
//        }
//
//        private static int WriteInt64(this Stream stream, long value)
//        {
//            BitCoder.WriteInt64(stream, value);
//            //stream.Write(BitConverter.GetBytes(value), 0, 8);
//            return 8;
//        }

        private static int Write(this Stream stream, bool value)
        {
            if (value)
            {
                stream.WriteByte(1);
            }
            else
            {
                stream.WriteByte(0);
            }
            return 1;
        }

        private static void WriteWithSize(this Stream stream, string value, byte[] buffer)
        {
            if (value == null)
            {
                stream.WriteVarInt32((int)0);
            }
            else if (string.IsNullOrWhiteSpace(value))
            {
                stream.WriteVarInt32((int)1);
            }
            else
            {
                var maxSize = System.Text.Encoding.Unicode.GetMaxByteCount(value.Length);
                if (buffer == null)
                {
                    buffer = new byte[maxSize];
                }
                else if (buffer.Length < maxSize)
                {
                    Array.Resize(ref buffer, maxSize);
                }
                var size = System.Text.Encoding.Unicode.GetBytes(value, 0, value.Length, buffer, 0);
                stream.WriteVarInt32(size + 2);
                stream.Write(buffer, 0, size);
            }
        }

        private static long ReadInt64(this Stream stream, byte[] buffer)
        {
            return stream.ReadInt64();
        }

        private static int ReadInt32(this Stream stream, byte[] buffer)
        {
            return stream.ReadInt32();
        }

        private static bool ReadBool(this Stream stream)
        {
            var v = stream.ReadByte();
            if (v == 0)
            {
                return false;
            }
            else if (v == 1)
            {
                return true;
            }
            else
            {
                throw new InvalidDataException("Cannot deserialize bool.");
            }
        }

        private static double ReadDouble(this Stream stream)
        {
            var value = stream.ReadInt64();
            return BitConverter.Int64BitsToDouble(value);
        }

        private static string ReadWithSizeString(this Stream stream, byte[] buffer)
        {
            var size = stream.ReadVarInt32();
            if (size == 0) return null;
            if (size == 1) return string.Empty;

            size -= 2;
            if (buffer.Length < size)
            {
                Array.Resize(ref buffer, size);
            }
            stream.Read(buffer, 0, size);

            return System.Text.Encoding.Unicode.GetString(buffer, 0, size);
        }

        private static void SkipStringWithSize(this Stream stream)
        {
            var size = stream.ReadVarInt32();
            if (size == 0) return;
            if (size == 1) return;
            
            size -= 2;
            stream.Seek(size, SeekOrigin.Current);
        }
    }
}