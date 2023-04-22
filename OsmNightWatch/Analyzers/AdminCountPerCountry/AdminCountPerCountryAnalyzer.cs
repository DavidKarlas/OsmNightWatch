using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System.Text.Json;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public partial class AdminCountPerCountryAnalyzer : IOsmAnalyzer
{
    private readonly CountriesTracker countriesTracker;


    public AdminCountPerCountryAnalyzer(KeyValueDatabase database)
    {
        this.database = database;
        relationChangesTracker = database.ReadRelationChangesTracker(1);
        this.countriesTracker = new CountriesTracker(database);
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


    private static void AssignAdminsToCountries(IOsmGeoBatchSource newOsmSource, STRtree<Country> strIndex, ProcessingAdmin admin)
    {
        var coordinates = new List<Coordinate>();
        var envelope = new Envelope();
        foreach (var member in admin.Admin.Members)
        {
            switch (member.Type)
            {
                case OsmGeoType.Node:
                    {
                        var node = newOsmSource.GetNode(member.Id);
                        var coordinate = node.ToCoordinate();
                        envelope.ExpandToInclude(coordinate);
                        coordinates.Add(coordinate);
                    }
                    break;
                case OsmGeoType.Way:
                    {
                        var way = newOsmSource.GetWay(member.Id);
                        admin.Ways.Add(way);
                        foreach (var nodeId in way.Nodes)
                        {
                            var node = newOsmSource.GetNode(nodeId);
                            var coordinate = node.ToCoordinate();
                            envelope.ExpandToInclude(coordinate);
                            coordinates.Add(coordinate);
                        }
                    }
                    break;
            }
        }

        foreach (var country in strIndex.Query(envelope))
        {
            foreach (var coordinate in coordinates)
            {
                if (country.Polygon.PointLocator.Locate(coordinate) == Location.Interior)
                {
                    admin.Countries.Add(country.RelationId);
                    break;
                }
            }
        }

        if (admin.Countries.Count == 1)
        {
            return;
        }
        admin.Countries.Clear();
        try
        {
            var adminPolygon = BuildPolygonFromRelation.BuildPolygon(admin.Admin, newOsmSource);
            foreach (var country in strIndex.Query(adminPolygon.Polygon.EnvelopeInternal))
            {
                if (country.Polygon.Contains(adminPolygon.Polygon))
                {
                    admin.Countries.Add(country.RelationId);
                    return;
                }
                var intersection = country.Polygon.Geometry.Intersection(adminPolygon.Polygon);
                if (intersection.Area > adminPolygon.Polygon.Area * 0.9)
                {
                    admin.Countries.Add(country.RelationId);
                }
            }
        }
        catch
        {
            // Ignore problems building the polygon
        }
    }

    RelationChangesTracker relationChangesTracker;
    private KeyValueDatabase database;

    public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
    {
        Utils.BatchLoad(relevantThings, newOsmSource, true, true);

        countriesTracker.ProcessPbf(relevantThings, newOsmSource);

        var relevantAdmins = relevantThings.Where(r => r.Tags.TryGetValue("admin_level", out var admLvl) && admLvl != "2").OfType<Relation>().Select(r => new ProcessingAdmin(r)).ToArray();
        Parallel.ForEach(relevantAdmins, new ParallelOptions() {
            MaxDegreeOfParallelism = 24
        }, (relation) => {
            AssignAdminsToCountries(newOsmSource, countriesTracker.StrIndex, relation);
        });

        var countriesDictionary = countriesTracker.Countries;
        countriesDictionary.Values.ToList().ForEach((c) => c.Admins.Clear());
        foreach (var admin in relevantAdmins)
        {
            relationChangesTracker.AddRelation(admin.Admin.Id, admin.Ways);

            var adminLevel = admin.Admin.Tags["admin_level"];

            var countryId = admin.Countries.SingleOrDefault();
            if (countryId == 0)
                continue;
            var admins = countriesDictionary[countryId].Admins;

            if (!admins.TryGetValue(adminLevel, out var adminLevelList))
            {
                admins[adminLevel] = adminLevelList = new List<long>();
            }
            adminLevelList.Add(admin.Admin.Id!);
        }

        var expectedState = JsonSerializer.Deserialize<StateOfTheAdmins>(new HttpClient().GetStringAsync("https://davidupload.blob.core.windows.net/data/current.json").Result);

        foreach (var expectedCountry in expectedState!.Countries)
        {
            if (!countriesDictionary.TryGetValue(expectedCountry.RelationId, out var country))
            {
                yield return new IssueData() {
                    IssueType = "MissingCountry",
                    FriendlyName = expectedCountry.EnglishName,
                    OsmType = "R",
                    OsmId = expectedCountry.RelationId
                };
                continue;
            }

            foreach (var trackedAdmins in expectedCountry.Admins)
            {
                foreach (var missingAdmin in trackedAdmins.Value.Except(country.Admins[trackedAdmins.Key]))
                {
                    yield return new IssueData() {
                        IssueType = "MissingAdmin",
                        FriendlyName = expectedCountry.EnglishName,
                        OsmType = "R",
                        OsmId = missingAdmin
                    };
                }

                foreach (var extraAdmin in country.Admins[trackedAdmins.Key].Except(trackedAdmins.Value))
                {
                    yield return new IssueData() {
                        IssueType = "ExtraAdmin",
                        FriendlyName = expectedCountry.EnglishName,
                        OsmType = "R",
                        OsmId = extraAdmin
                    };
                }
            }
        }

        database.WriteRelationChangesTracker(1, relationChangesTracker);
    }

    public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoSource newOsmSource)
    {
        countriesTracker.ProcessChangeset(changeSet, newOsmSource);

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

        database.WriteRelationChangesTracker(1, relationChangesTracker);
        return Array.Empty<IssueData>();
    }
}
