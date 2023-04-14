using OsmNightWatch;
using OsmNightWatch.Analyzers;
using OsmNightWatch.Lib;
using OsmSharp;

internal class bla : IIncrementalOsmAnalyzer
{
    public string AnalyzerName => "bla";

    public FilterSettings FilterSettings => new FilterSettings() {
        Filters = new List<ElementFilter> {
             new ElementFilter(OsmGeoType.Node,new List<TagFilter> {new TagFilter("name")}),
             new ElementFilter(OsmGeoType.Way,new List<TagFilter> {new TagFilter("name")}),
             new ElementFilter(OsmGeoType.Relation,new List<TagFilter> {new TagFilter("name")})
         }
    };

    public Func<OsmGeo, IssueData?> GetValidator()
    {
        return (OsmGeo osmGeo) => {

            if (osmGeo.Tags["name"] == "*")
            {
                return new IssueData() {
                    OsmType = osmGeo.Type.ToString(),
                    OsmId = (long)osmGeo.Id!,
                    Details = osmGeo.Tags["name"],
                    IssueType = "Invalid characters",
                    FriendlyName = osmGeo.Tags["name"]
                };
            }
            return null;
        };
    }
}