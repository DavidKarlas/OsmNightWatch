using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Lib;
using OsmSharp;

internal class bla : IOsmAnalyzer
{
    public string AnalyzerName => "bla";

    public FilterSettings FilterSettings => new FilterSettings() {
        Filters = new List<ElementFilter> {
             //new ElementFilter(OsmGeoType.Node,new List<TagFilter> {new TagFilter("name")}),
             new ElementFilter(OsmGeoType.Way,new List<TagFilter> {new TagFilter("name")}),
             new ElementFilter(OsmGeoType.Relation,new List<TagFilter> {new TagFilter("name")})
         }
    };

    public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
    {
        foreach (var thing in relevantThings)
        {
            if (thing.Tags["name"].Contains("fuck", StringComparison.InvariantCultureIgnoreCase))
            {
                yield return new IssueData() { IssueType = "fuck", FriendlyName = thing.Tags["name"] };
            }
        }
    }
}