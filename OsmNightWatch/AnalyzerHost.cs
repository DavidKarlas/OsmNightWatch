using OsmNightWatch.Analyzers;
using OsmNightWatch.PbfParsing;

internal class AnalyzerHost
{
    private IOsmAnalyzer analyzer;
    private PbfDatabaseWithProcessedChangesets pbfDatabaseWithProcessedChangesets;

    public AnalyzerHost(IOsmAnalyzer analyzer, PbfDatabaseWithProcessedChangesets pbfDatabaseWithProcessedChangesets)
    {
        this.analyzer = analyzer;
        this.pbfDatabaseWithProcessedChangesets = pbfDatabaseWithProcessedChangesets;
    }

    public async Task InitializeAsync()
    {
        var relevantRelations = RelationsParser.Parse(analyzer.GetFilters(), index);
        var waysToLoad = new HashSet<long>(relevantRelations.Values.SelectMany(r => r.Members).Where(m => m.Type == OsmSharp.OsmGeoType.Way).Select(w => w.Id));
        var relevantWays = WaysParser.LoadWays(waysToLoad, index);
        var nodesToLoad = new HashSet<long>(relevantRelations.Values.SelectMany(r => r.Members).Where(m => m.Type == OsmSharp.OsmGeoType.Node).Select(n => n.Id));
        nodesToLoad.UnionWith(relevantWays.Values.SelectMany(w => w.Nodes));
        var nodes = NodesParser.LoadNodes(nodesToLoad, index);
    }
}