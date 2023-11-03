using OsmNightWatch;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System.Collections.Concurrent;

internal class PbfDatabase : IOsmValidateSource
{
    private PbfIndex index;


    public PbfDatabase(PbfIndex index)
    {
        this.index = index;
    }

    public (IReadOnlyCollection<Node> nodes, IReadOnlyCollection<Way> ways, IReadOnlyCollection<Relation> relations) BatchLoad(HashSet<long>? nodeIds = null, HashSet<long>? wayIds = null, HashSet<long>? relationIds = null)
    {
        return (NodesParser.LoadNodes(nodeIds, index), WaysParser.LoadWays(wayIds, index), RelationsParser.LoadRelations(relationIds, index));
    }

    public IEnumerable<OsmGeo> Filter(FilterSettings filterSettings)
    {
        foreach (var group in filterSettings.Filters.Where(f => f.Tags != null).GroupBy(f => f.GeoType))
        {
            switch (group.Key)
            {
                case OsmGeoType.Node:
                    foreach (var node in NodesParser.Parse(group, index))
                    {
                        yield return node;
                    }
                    break;
                case OsmGeoType.Way:
                    foreach (var way in WaysParser.Parse(group, index))
                    {
                        yield return way;
                    }
                    break;
                case OsmGeoType.Relation:
                    foreach (var relation in RelationsParser.Parse(group, index))
                    {
                        yield return relation;
                    }
                    break;
            }
        }
        foreach (var group in filterSettings.Filters.Where(f => f.Ids != null).GroupBy(f => f.GeoType))
        {
            switch (group.Key)
            {
                case OsmGeoType.Node:
                    var nodeIds = new HashSet<long>();
                    foreach (var filter in group)
                    {
                        nodeIds.UnionWith(filter.Ids!);
                    }
                    foreach (var node in NodesParser.LoadNodes(nodeIds, index))
                    {
                        yield return node;
                    }
                    break;
                case OsmGeoType.Way:
                    var wayIds = new HashSet<long>();
                    foreach (var filter in group)
                    {
                        wayIds.UnionWith(filter.Ids!);
                    }
                    foreach (var way in WaysParser.LoadWays(wayIds, index))
                    {
                        yield return way;
                    }
                    break;
                case OsmGeoType.Relation:
                    var relationIds = new HashSet<long>();
                    foreach (var filter in group)
                    {
                        relationIds.UnionWith(filter.Ids!);
                    }
                    foreach (var relation in RelationsParser.LoadRelations(relationIds, index))
                    {
                        yield return relation;
                    }
                    break;
            }
        }
    }

    public IEnumerable<IssueData> Validate(Func<OsmGeo, IssueData?> validator, FilterSettings filterSettings)
    {
        var issues = new ConcurrentBag<IssueData>();
        //NodesParser.Process((way) => {
        //    if (validator(way) is IssueData issue)
        //    {
        //        issues.Add(issue);
        //    }
        //}, filterSettings.Filters, index);
        //WaysParser.Process((way) => {
        //    if (validator(way) is IssueData issue)
        //    {
        //        issues.Add(issue);
        //    }
        //}, filterSettings.Filters, index);
        return issues;
    }

    public OsmGeo Get(OsmGeoType type, long id)
    {
        switch (type)
        {
            case OsmGeoType.Node:
                return NodesParser.LoadNodes(new HashSet<long>() { id }, index).Single();
            case OsmGeoType.Way:
                return WaysParser.LoadWays(new HashSet<long>() { id }, index).Single();
            case OsmGeoType.Relation:
                return RelationsParser.LoadRelations(new HashSet<long>() { id }, index).Single();
            default:
                throw new NotImplementedException();
        }
    }

    public Node GetNode(long id)
    {
        return NodesParser.LoadNodes(new HashSet<long>() { id }, index).Single();
    }

    public Way GetWay(long id)
    {
        return WaysParser.LoadWays(new HashSet<long>() { id }, index).Single();
    }

    public Relation GetRelation(long id)
    {
        return RelationsParser.LoadRelations(new HashSet<long>() { id }, index).Single();
    }
}