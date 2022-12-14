using OsmNightWatch;
using OsmNightWatch.PbfParsing;
using OsmSharp;

internal class PbfDatabase : IOsmGeoFilterableSource
{
    private PbfIndex index;

    private readonly Dictionary<long, Node> _nodes = new();
    private readonly Dictionary<long, Way> _ways = new();
    private readonly Dictionary<long, Relation> _relations = new();

    public PbfDatabase(PbfIndex index)
    {
        this.index = index;
    }

    public void BatchLoad(HashSet<long>? nodeIds = null, HashSet<long>? wayIds = null, HashSet<long>? relationIds = null)
    {
        if (nodeIds != null)
        {
            nodeIds.ExceptWith(_nodes.Keys);
            if (nodeIds.Count > 0)
            {
                foreach (var node in NodesParser.LoadNodes(nodeIds, index))
                {
                    _nodes[node.Key] = node.Value;
                }
            }
        }
        if (wayIds != null)
        {
            wayIds.ExceptWith(_ways.Keys);
            if (wayIds.Count > 0)
            {
                foreach (var way in WaysParser.LoadWays(wayIds, index))
                {
                    _ways[way.Key] = way.Value;
                }
            }
        }
        if (relationIds != null)
        {
            relationIds.ExceptWith(_relations.Keys);
            if (relationIds.Count > 0)
            {
                foreach (var relation in RelationsParser.LoadRelations(relationIds, index))
                {
                    _relations[relation.Key] = relation.Value;
                }
            }
        }
    }

    public OsmGeo Get(OsmGeoType type, long id)
    {
        switch (type)
        {
            case OsmGeoType.Node:
                if (_nodes.TryGetValue(id, out var node))
                    return node;
                node = NodesParser.LoadNodes(new HashSet<long>() { id }, index).Values.Single();
                _nodes[(long)node.Id!] = node;
                return node;
            case OsmGeoType.Way:
                if (_ways.TryGetValue(id, out var way))
                    return way;
                way = WaysParser.LoadWays(new HashSet<long>() { id }, index).Values.Single();
                _ways[(long)way.Id!] = way;
                return way;
            case OsmGeoType.Relation:
                if (_relations.TryGetValue(id, out var relation))
                    return relation;
                relation = RelationsParser.LoadRelations(new HashSet<long>() { id }, index).Values.Single();
                _relations[(long)relation.Id!] = relation;
                return relation;
            default:
                throw new NotImplementedException();
        }
    }

    public IEnumerable<OsmGeo> Filter(FilterSettings filterSettings)
    {
        foreach (var group in filterSettings.Filters.GroupBy(f => f.GeoType))
        {
            switch (group.Key)
            {
                case OsmGeoType.Node:
                    throw new NotImplementedException();
                case OsmGeoType.Way:
                    foreach (var way in WaysParser.Parse(group, index))
                    {
                        _ways[(long)way.Id!] = way;
                        yield return way;
                    }
                    break;
                case OsmGeoType.Relation:
                    foreach (var relation in RelationsParser.Parse(group, index))
                    {
                        _relations[(long)relation.Id!] = relation;
                        yield return relation;
                    }
                    break;
            }
        }
    }
}