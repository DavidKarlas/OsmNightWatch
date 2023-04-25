using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public partial class AdminCountPerCountryAnalyzer : IOsmAnalyzer
{

    RelationChangesTracker relationChangesTracker;
    private KeyValueDatabase database;

    public AdminCountPerCountryAnalyzer(KeyValueDatabase database)
    {
        this.database = database;
        relationChangesTracker = database.ReadRelationChangesTracker(1);
    }

    public string AnalyzerName => nameof(AdminCountPerCountryAnalyzer);

    public FilterSettings FilterSettings { get; } = new FilterSettings() {
        Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Relation, new[] {
                    new TagFilter("boundary", "administrative"),
                    new TagFilter("type", "boundary"),
                    new TagFilter("admin_level", "2", "3","4","5","6","7","8") },
                    true,
                    true)
            }
    };

    private void UpdateRelations(IEnumerable<(long Id, Relation Relation)> relevantThings, IOsmGeoSource newOsmSource)
    {
        Parallel.ForEach(relevantThings, new ParallelOptions() {
            MaxDegreeOfParallelism = 32
        }, (admin) => {
            if (admin.Relation == null)
            {
                database.DeleteAdmin(admin.Id).Wait();
                return;
            }
            var polygon = BuildPolygonFromRelation.BuildPolygon(admin.Relation, newOsmSource);
            relationChangesTracker.AddRelation(admin.Id, polygon.Ways, newOsmSource);
            if (!polygon.Polygon.IsEmpty && admin.Relation.Tags!.TryGetValue("admin_level", out var admLvl) && int.TryParse(admLvl, out var admLvlInt))
            {
                database.UpsertAdmin(admin.Id, admLvlInt, polygon.Polygon).Wait();
            }
            else
            {
                database.DeleteAdmin(admin.Id).Wait();
            }
        });

        database.WriteRelationChangesTracker(1, relationChangesTracker);
    }

    public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
    {
        Utils.BatchLoad(relevantThings, newOsmSource, true, true);
        UpdateRelations(relevantThings.Select(r => (r.Id, (Relation)r)), newOsmSource);
        return Array.Empty<IssueData>();
    }

    public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoSource newOsmSource)
    {
        var filter = FilterSettings.Filters.Single();

        var changedRelations = relationChangesTracker.GetChangedRelations(changeSet);
        foreach (var relation in changeSet.Relations.Values)
        {
            if (relation == null)
                continue;
            if (filter.Matches(relation))
            {
                changedRelations.Add(relation.Id);
            }
        }

        UpdateRelations(changedRelations.Select(id => (id, newOsmSource.GetRelation(id))), newOsmSource);

        var expectedState = JsonSerializer.Deserialize<StateOfTheAdmins>(new HttpClient().GetStringAsync("https://davidupload.blob.core.windows.net/data/current.json").Result);

        foreach (var expectedCountry in expectedState.Countries)
        {
            var countryExists = database.DoesCountryExist(expectedCountry.RelationId);
            if (!countryExists)
            {
                yield return new IssueData() {
                    IssueType = "MissingCountry",
                    FriendlyName = expectedCountry.EnglishName,
                    OsmType = "R",
                    OsmId = expectedCountry.RelationId
                };
                continue;
            }
            foreach (var adminLevel in expectedCountry.Admins)
            {
                var sw = Stopwatch.StartNew();
                var actualAdmins = database.GetCountryAdmins(expectedCountry.RelationId, int.Parse(adminLevel.Key));

                foreach (var missingAdmin in adminLevel.Value.Except(actualAdmins))
                {
                    yield return new IssueData() {
                        IssueType = "MissingAdmin",
                        FriendlyName = expectedCountry.EnglishName,
                        OsmType = "R",
                        OsmId = missingAdmin
                    };
                }

                foreach (var extraAdmin in actualAdmins.Except(adminLevel.Value))
                {
                    yield return new IssueData() {
                        IssueType = "ExtraAdmin",
                        FriendlyName = expectedCountry.EnglishName,
                        OsmType = "R",
                        OsmId = extraAdmin
                    };
                }
                Console.WriteLine(sw.Elapsed + " " + expectedCountry.EnglishName + " Lvl:" + adminLevel.Key + " Total:" + actualAdmins.Count + " Extra:" + actualAdmins.Except(adminLevel.Value).Count() + " Missing:" + adminLevel.Value.Except(actualAdmins).Count());
            }
        }
    }
}
