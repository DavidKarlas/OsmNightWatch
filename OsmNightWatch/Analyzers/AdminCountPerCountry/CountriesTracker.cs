using NetTopologySuite.Index.Strtree;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
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

        RelationChangesTracker relationChangesTracker;
        private KeyValueDatabase database;

        public CountriesTracker(KeyValueDatabase database)
        {
            this.database = database;
            relationChangesTracker = database.ReadRelationChangesTracker(2);
        }

        public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
        {
            var filter = FilterSettings.Filters.Single();
            Parallel.ForEach(relevantThings, (geo) => {
                if (!filter.Matches(geo))
                    return;

                var country = new Country() {
                    EnglishName = geo.Tags["name:en"],
                    Iso2 = geo.Tags["ISO3166-1:alpha2"],
                    Iso3 = geo.Tags["ISO3166-1:alpha3"],
                    RelationId = geo.Id
                };
                var polygon = BuildPolygonFromRelation.BuildPolygon((Relation)geo, newOsmSource);
                country.Polygon = new NetTopologySuite.Geometries.Prepared.PreparedPolygon(polygon.Polygon);

                lock (Countries)
                {
                    relationChangesTracker.AddRelation(country.RelationId, polygon.Ways);
                    Countries[country.RelationId] = country;
                }
            });
#if DEBUG
            foreach (var country in Countries.Values.OrderBy(c => c.EnglishName))
            {
                Console.WriteLine(country.EnglishName);
            }
#endif
            database.WriteRelationChangesTracker(2, relationChangesTracker);
            return Array.Empty<IssueData>();
        }

        public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoSource newOsmSource)
        {
            var changedRelations = relationChangesTracker.GetChangedRelations(changeSet);
            var filter = FilterSettings.Filters.Single();
            foreach (var relation in changeSet.Relations.Values)
            {
                if (relation == null)
                    continue;
                if (filter.Matches(relation))
                {
                    changedRelations.Add(relation.Id);
                }
            }

            if (changedRelations.Count == 0)
            {
                return Array.Empty<IssueData>();
            }

            Parallel.ForEach(changedRelations, (relationId) => {
                var geo = newOsmSource.GetRelation(relationId);
                if (!filter.Matches(geo))
                {
                    lock (Countries)
                    {
                        Countries.Remove(relationId);
                    }
                    return;
                }

                var country = new Country() {
                    EnglishName = geo.Tags["name:en"],
                    Iso2 = geo.Tags["ISO3166-1:alpha2"],
                    Iso3 = geo.Tags["ISO3166-1:alpha3"],
                    RelationId = geo.Id
                };
                var polygon = BuildPolygonFromRelation.BuildPolygon(geo, newOsmSource);
                country.Polygon = new NetTopologySuite.Geometries.Prepared.PreparedPolygon(polygon.Polygon);

                lock (Countries)
                {
                    relationChangesTracker.AddRelation(country.RelationId, polygon.Ways);
                    Countries[country.RelationId] = country;
                }
            });

            database.WriteRelationChangesTracker(2, relationChangesTracker);
            return Array.Empty<IssueData>();
        }
    }
}
