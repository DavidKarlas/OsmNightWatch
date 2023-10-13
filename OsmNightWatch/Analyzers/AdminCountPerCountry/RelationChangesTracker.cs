using OsmNightWatch.PbfParsing;
using System.Buffers.Binary;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public class RelationChangesTracker
{
    public Dictionary<long, HashSet<uint>> NodeToWay = new();
    public Dictionary<uint, HashSet<uint>> WayToRelation = new();
    public HashSet<uint> Relations = new();
    public HashSet<uint> TrackedRelations = new();
    private KeyValueDatabase database;

    public RelationChangesTracker(KeyValueDatabase database)
    {
        this.database = database;
    }

    public void AddRelationToTrack(uint relationId, List<Way> ways)
    {
        lock (Relations)
        {
            if (TrackedRelations.Add(relationId))
                Relations.Add(relationId);
            foreach (var way in ways)
            {
                if (WayToRelation.TryGetValue((uint)way.Id!, out var relations))
                {
                    relations.Add(relationId);
                }
                else
                {
                    WayToRelation.Add((uint)way.Id!, new HashSet<uint>() { relationId });
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
    }

    public void AddWaysToTrack(IEnumerable<Way> ways)
    {
        lock (Relations)
        {
            foreach (var way in ways)
            {
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

    public HashSet<uint> GetChangedWays(MergedChangeset changeSet)
    {
        var waysSet = new HashSet<uint>();
        foreach (var node in changeSet.OsmNodes)
        {
            if (node.Value == null || node.Value.Version < 2)
            {
                continue;
            }
            database.GetNodeToWay(node.Key, waysSet);
        }

        foreach (var way in changeSet.OsmWays)
        {
            if (way.Value == null || way.Value.Version < 2)
            {
                continue;
            }
            waysSet.Add((uint)way.Key);
        }

        return waysSet;
    }

    public HashSet<uint> GetChangedRelations(MergedChangeset changeSet)
    {
        var result = new HashSet<uint>();
        var waysSet = GetChangedWays(changeSet);

        foreach (var wayId in waysSet)
        {
            database.GetWayToRelation(wayId, result);
        }

        foreach (var relation in changeSet.Relations.Keys)
        {
            if (TrackedRelations.Contains(relation))
            {
                result.Add(relation);
            }
        }

        return result;
    }
}
