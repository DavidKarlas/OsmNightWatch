using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using OsmNightWatch.Lib;
using OsmSharp;
using OsmSharp.Db;
using System.Text.Json;

namespace OsmNightWatch.Analyzers.AdminCountPerAdmin2;

public partial class AdminCountPerAdmin2Analyzer : IOsmAnalyzer {
    public string AnalyzerName => nameof(AdminCountPerAdmin2Analyzer);

    public FilterSettings FilterSettings { get; } = new FilterSettings() {
        Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Relation, new[] {
                    new TagFilter("boundary", "administrative"),
                    new TagFilter("type", "boundary"),
                    new TagFilter("admin_level", "3","4","5","6","7","8") },
                    true,
                    true)
            }
    };


    public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource) {
        var state = new StateOfTheAdmins();
        Utils.BatchLoad(relevantThings, newOsmSource, true, true);
        
        var relevantAdmins = relevantThings.Where(r => r.Tags.TryGetValue("admin_level", out var admLvl) && admLvl != "2" && r.Id != 1473938).OfType<Relation>().Select(r => new ProcessingAdmin(r)).ToArray();
        Parallel.ForEach(relevantAdmins, new ParallelOptions() {
            MaxDegreeOfParallelism = 48
        }, (relation) => {
            AssignAdminsToCountries(newOsmSource, strIndex, relation);
        });

        var countriesDictionary = state.Countries.ToDictionary(c => c.RelationId);
        foreach (var admin in relevantAdmins)
        {
            string adminLevel = admin.Admin.Tags["admin_level"];

            var countryId = admin.Countries.SingleOrDefault();
            if (countryId == 0)
                continue;
            var admins = countriesDictionary[countryId].Admins;

            if (!admins.TryGetValue(adminLevel, out var adminLevelList))
            {
                admins[adminLevel] = adminLevelList = new List<long>();
            }
            adminLevelList.Add((long)admin.Admin.Id!);
        }

        var expectedState = JsonSerializer.Deserialize<StateOfTheAdmins>(new HttpClient().GetStringAsync("https://davidupload.blob.core.windows.net/data/current.json").Result);

        foreach (var expectedCountry in expectedState!.Countries)
        {
            var country = state.Countries.SingleOrDefault(c => c.RelationId == expectedCountry.RelationId);
            if (country == null)
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
    }

    private static void AssignAdminsToCountries(IOsmGeoBatchSource newOsmSource, STRtree<ProcessingCountry> strIndex, ProcessingAdmin admin) {
        foreach (var coordinate in ExtractCoordinates.ExtractCoordinatesFromRelation(admin.Admin, newOsmSource))
        {
            foreach (var country in strIndex.Query(new Envelope(coordinate)))
            {
                if (country.Polygon.ContainsProperly(new Point(coordinate)))
                {
                    admin.Countries.Add(country.Country.RelationId);
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
            foreach (var country in strIndex.Query(adminPolygon.EnvelopeInternal))
            {
                if (country.Polygon.Contains(adminPolygon))
                {
                    admin.Countries.Add(country.Country.RelationId);
                    return;
                }
                var intersection = country.Polygon.Geometry.Intersection(adminPolygon);
                if (intersection.Area > adminPolygon.Area * 0.9)
                {
                    admin.Countries.Add(country.Country.RelationId);
                }
            }
        }
        catch
        {
            // Ignore problems building the polygon
        }
    }
}
