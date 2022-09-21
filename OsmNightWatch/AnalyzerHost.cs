using OsmNightWatch.Analyzers;
using OsmNightWatch.PbfParsing;
using OsmSharp.Db;

public class AnalyzerHost
{
    public IOsmAnalyzer Analyzer { get; }
    private IOsmGeoSource geoSource;

    public AnalyzerHost(IOsmAnalyzer analyzer, IOsmGeoSource geoSource)
    {
        this.Analyzer = analyzer;
        this.geoSource = geoSource;
    }

    public async Task InitializeAsync()
    {
        //var relevantRelations = RelationsParser.Parse(analyzer.GetFilters(), geoSource);
        //var waysToLoad = new HashSet<long>(relevantRelations.Values.SelectMany(r => r.Members).Where(m => m.Type == OsmSharp.OsmGeoType.Way).Select(w => w.Id));
        //var relevantWays = WaysParser.LoadWays(waysToLoad, index);
        //var nodesToLoad = new HashSet<long>(relevantRelations.Values.SelectMany(r => r.Members).Where(m => m.Type == OsmSharp.OsmGeoType.Node).Select(n => n.Id));
        //nodesToLoad.UnionWith(relevantWays.Values.SelectMany(w => w.Nodes));
        //var nodes = NodesParser.LoadNodes(nodesToLoad, index);
    }
}