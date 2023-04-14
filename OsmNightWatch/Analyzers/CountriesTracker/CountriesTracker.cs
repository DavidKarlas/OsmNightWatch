using OsmNightWatch.Lib;
using OsmSharp;

namespace OsmNightWatch.Analyzers.CountriesTracker {
    public class CountriesTracker
    {
        public FilterSettings FilterSettings { get; } = new FilterSettings() {
            Filters = new List<ElementFilter>()
                {
                new ElementFilter(OsmGeoType.Relation, new[] {
                    new TagFilter("boundary", "administrative"),
                    new TagFilter("type", "boundary"),
                    new TagFilter("admin_level", "2") },
                    true,
                    true)
            }
        };

        public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource) {
            throw new NotImplementedException();
        }
    }
}
