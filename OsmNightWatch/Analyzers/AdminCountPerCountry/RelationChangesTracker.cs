using OsmNightWatch.PbfParsing;
using System.Buffers.Binary;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public class RelationChangesTracker
{
    Dictionary<long, HashSet<uint>> NodeToWay = new();
    Dictionary<uint, HashSet<uint>> WayToRelation = new();
    HashSet<long> Relations = new();

    public void AddRelation(long relationId, List<Way> ways)
    {
        Relations.Add(relationId);
        foreach (var way in ways)
        {
            if (WayToRelation.TryGetValue((uint)way.Id!, out var relations))
            {
                relations.Add((uint)relationId);
            }
            else
            {
                WayToRelation.Add((uint)way.Id!, new HashSet<uint>() { (uint)relationId });
                foreach (var node in way.Nodes)
                {
                    if (NodeToWay.TryGetValue(node, out var nodeToWayWays))
                    {
                        nodeToWayWays.Add((uint)way.Id!);
                    }
                    else
                    {
                        NodeToWay.Add(node, new HashSet<uint>() { (uint)way.Id! });
                    }
                }
            }
        }
    }

    public RelationChangesTracker(string? existingPath = null)
    {
        if (string.IsNullOrEmpty(existingPath))
        {
            return;
        }

        ReadOnlySpan<byte> span = File.ReadAllBytes(existingPath);
        var relationsCount = BinSerialize.ReadInt(ref span);
        for (int i = 0; i < relationsCount; i++)
        {
            Relations.Add(BinSerialize.ReadUInt(ref span));
        }
        var wayToRelationCount = BinSerialize.ReadInt(ref span);
        for (int i = 0; i < wayToRelationCount; i++)
        {
            var way = BinSerialize.ReadUInt(ref span);
            var hashset = new HashSet<uint>();
            var count = BinSerialize.ReadByte(ref span);
            for (int j = 0; j < count; j++)
            {
                hashset.Add(BinSerialize.ReadUInt(ref span));
            }
            WayToRelation.Add(way, hashset);
        }
        var nodeToWayCount = BinSerialize.ReadInt(ref span);
        for (int i = 0; i < nodeToWayCount; i++)
        {
            var node = BinSerialize.ReadLong(ref span);
            var hashset = new HashSet<uint>();
            var count = BinSerialize.ReadByte(ref span);
            for (int j = 0; j < count; j++)
            {
                hashset.Add(BinSerialize.ReadUInt(ref span));
            }
            NodeToWay.Add(node, hashset);
        }
    }

    public byte[] Serialize()
    {
        var memoryStream = new MemoryStream();
        memoryStream.Write(BitConverter.GetBytes(Relations.Count));
        foreach (var relation in Relations)
        {
            memoryStream.Write(BitConverter.GetBytes(relation));
        }
        memoryStream.Write(BitConverter.GetBytes(WayToRelation.Count));
        foreach (var relation in WayToRelation)
        {
            memoryStream.Write(BitConverter.GetBytes(relation.Key));
            memoryStream.Write(BitConverter.GetBytes((byte)relation.Value.Count));
            foreach (var item in relation.Value)
            {
                memoryStream.Write(BitConverter.GetBytes(item));
            }
        }
        foreach (var relation in NodeToWay)
        {
            memoryStream.Write(BitConverter.GetBytes(relation.Key));
            memoryStream.Write(BitConverter.GetBytes((byte)relation.Value.Count));
            foreach (var item in relation.Value)
            {
                memoryStream.Write(BitConverter.GetBytes(item));
            }
        }
        memoryStream.Position = 0;
        return memoryStream.ToArray();
    }


    public HashSet<long> GetChangedRelations(MergedChangeset changeSet)
    {
        var result = new HashSet<long>();
        foreach (var node in changeSet.Nodes.Keys)
        {
            if (NodeToWay.TryGetValue(node, out var ways))
            {
                foreach (var way in ways)
                {
                    if (WayToRelation.TryGetValue(way, out var relations))
                    {
                        foreach (var relation in relations)
                        {
                            result.Add(relation);
                        }
                    }
                }
            }
        }

        foreach (var way in changeSet.Ways.Keys)
        {
            if (WayToRelation.TryGetValue((uint)way, out var relations))
            {
                foreach (var relation in relations)
                {
                    result.Add(relation);
                }
            }
        }

        foreach (var relation in changeSet.Relations.Keys)
        {
            if (Relations.Contains((uint)relation))
            {
                result.Add(relation);
            }
        }

        return result;
    }
}
