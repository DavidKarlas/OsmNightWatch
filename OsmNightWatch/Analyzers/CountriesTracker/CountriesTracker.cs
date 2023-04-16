using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using OsmNightWatch.Analyzers.AdminCountPerAdmin2;
using OsmNightWatch.Lib;
using OsmSharp;

namespace OsmNightWatch.Analyzers.CountriesTracker
{
    public class CountriesTracker : IOsmAnalyzer
    {
        public FilterSettings FilterSettings { get; } = new FilterSettings() {
            Filters = new List<ElementFilter>()
                {
                new ElementFilter(OsmGeoType.Relation, new[] {
                    new TagFilter("boundary", "administrative"),
                    new TagFilter("type", "boundary"),
                    new TagFilter("admin_level", "2"),
                    new TagFilter("name:en"),
                    new TagFilter("ISO3166-1:alpha2"),
                    new TagFilter("ISO3166-1:alpha3") },
                    true,
                    true)
            }
        };

        public string AnalyzerName => nameof(CountriesTracker);

        public Func<OsmGeo, IssueData?> GetValidator() => Validate;

        public Dictionary<long, Country> Countries = new Dictionary<long, Country>();

        STRtree<Country>? strTree;
        public STRtree<Country> StrIndex
        {
            get
            {
                if (strTree is STRtree<Country> tree)
                {
                    return tree;
                }
                tree = new STRtree<Country>(250);
                foreach (var country in Countries.Values)
                {
                    tree.Insert(country.Polygon.Geometry.EnvelopeInternal, country);
                }
                tree.Build();
                strTree = tree;
                return tree;
            }
        }

        private IssueData? Validate(OsmGeo geo)
        {
            var country = new Country() {
                EnglishName = geo.Tags["name:en"],
                Iso2 = geo.Tags["ISO3166-1:alpha2"],
                Iso3 = geo.Tags["ISO3166-1:alpha3"],
                RelationId = (long)geo.Id!
            };

            country.Polygon = BuildPolygonFromRelation.BuildPolygon((Relation)geo, newOsmSource);

            if (country.Polygon.Geometry == MultiPolygon.Empty)
            {
                return new IssueData() {
                    IssueType = "CountryMissingPolygon",
                    FriendlyName = country.EnglishName,
                    OsmType = "R",
                    OsmId = country.RelationId
                };
            }

            lock (Countries)
            {
                Countries[country.RelationId] = country;
            }

            return null;
        }
    }
}
