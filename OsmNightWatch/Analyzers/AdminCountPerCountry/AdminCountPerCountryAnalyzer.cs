using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System.Text.Json;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public partial class AdminCountPerCountryAnalyzer : IOsmAnalyzer
{

    RelationChangesTracker relationChangesTracker;
    private KeyValueDatabase database;

    public AdminCountPerCountryAnalyzer(KeyValueDatabase database)
    {
        this.database = database;
        relationChangesTracker = database.ReadRelationChangesTracker();
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

    private IEnumerable<IssueData> UpdateRelations((uint Id, Relation Relation)[] relevantThings, IOsmGeoSource newOsmSource)
    {
        Parallel.ForEach(relevantThings, new ParallelOptions() {
            MaxDegreeOfParallelism = 32
        }, (admin) => {
            // Drop admin from database if it was deleted or doesn't meet admin criteria
            if (admin.Relation == null || admin.Relation.Tags == null || !FilterSettings.Filters.Single().Matches(admin.Relation))
            {
                database.DeleteAdmin(admin.Id);
                return;
            }
            var result = BuildPolygonFromRelation.BuildPolygon(admin.Relation, newOsmSource);
            relationChangesTracker.AddRelation(admin.Id, result.Ways);
            NetTopologySuite.Geometries.Geometry? geom;
            if (result.reason != null)
            {
                geom = null;
            }
            else if (result.Polygon.IsEmpty)
            {
                geom = null;
                result.reason = "Polygon is broken.";
            }
            else if (!result.Polygon.IsValid)
            {
                geom = null;
                result.reason = "Polygon is not valid.";
            }
            else
            {
                geom = result.Polygon;
            }
            if (!admin.Relation.Tags.TryGetValue("name:en", out var friendlyName))
            {
                if (!admin.Relation.Tags.TryGetValue("name", out friendlyName))
                {
                    friendlyName = "";
                }
            }
            database.UpsertAdmin(admin.Id, friendlyName, int.Parse(admin.Relation.Tags!["admin_level"]), geom, result.reason);
        });

        database.WriteRelationChangesTracker(relationChangesTracker);

        var expectedState = JsonSerializer.Deserialize<StateOfTheAdmins>(new HttpClient().GetStringAsync("https://davidupload.blob.core.windows.net/data/current.json").Result, new JsonSerializerOptions() {
            ReadCommentHandling = JsonCommentHandling.Skip
        });

        if (currentState == null)
        {
            currentState = CreateCurrentState(expectedState!);
        }
        else
        {
            currentState = UpdateCurrentState(currentState, relevantThings);
        }

        foreach (var (relationId, name, adminLevel, reason) in database.GetBrokenAdmins())
        {
            if (reason == "Missing ways")
            {
                // Special handling for relations without ways
                // that are often in Africa where relation consists of places nodes
                // we can't fix them because no source for borders but also don't want
                // to delete in OSM, but want to keep track...
                yield return new IssueData() {
                    IssueType = "MissingWays",
                    FriendlyName = $"{name}({adminLevel})",
                    OsmType = "R",
                    OsmId = relationId
                };
            }
            else
            {
                var issueType = "OpenAdminPolygon";
                if (adminLevel > 6)
                {
                    issueType += adminLevel;
                }
                yield return new IssueData() {
                    IssueType = issueType,
                    FriendlyName = name,
                    OsmType = "R",
                    OsmId = relationId,
                    Details = reason
                };
            }
        }

        var currentCountries = currentState.Countries.ToDictionary(c => c.RelationId);
        foreach (var expectedCountry in expectedState.Countries)
        {
            var country = currentCountries[expectedCountry.RelationId];
            if (!country.IsValid)
            {
                yield return new IssueData() {
                    IssueType = "AdminsState",
                    FriendlyName = "Missing " + expectedCountry.EnglishName,
                    OsmType = "R",
                    OsmId = expectedCountry.RelationId
                };
                continue;
            }
            foreach (var expectedAdmins in expectedCountry.Admins)
            {
                var actualAdmins = country.Admins[expectedAdmins.Key];

                foreach (var missingAdmin in expectedAdmins.Value.Except(actualAdmins))
                {
                    yield return new IssueData() {
                        IssueType = "AdminsState",
                        FriendlyName = $"{expectedCountry.EnglishName}({expectedAdmins.Key}) lost {missingAdmin}",
                        OsmType = "R",
                        OsmId = missingAdmin
                    };
                }

                foreach (var extraAdmin in actualAdmins.Except(expectedAdmins.Value))
                {
                    yield return new IssueData() {
                        IssueType = "AdminsState",
                        FriendlyName = $"{expectedCountry.EnglishName}({expectedAdmins.Key}) gained {extraAdmin}",
                        OsmType = "R",
                        OsmId = extraAdmin
                    };
                }
            }
        }
    }

    public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
    {
        Utils.BatchLoad(relevantThings, newOsmSource, true, true);
        return UpdateRelations(relevantThings.Select(r => ((uint)r.Id, (Relation)r)).ToArray(), newOsmSource);
    }

    StateOfTheAdmins? currentState;

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
                changedRelations.Add((uint)relation.Id);
            }
        }
        //changedRelations.Add(3286892);// for testing specific relation build polygon logic
        return UpdateRelations(changedRelations.Select(id => (id, newOsmSource.GetRelation(id))).ToArray(), newOsmSource);
    }

    private StateOfTheAdmins UpdateCurrentState(StateOfTheAdmins oldState, (uint Id, Relation Relation)[] relevantThings)
    {
        var thingsThatNeedReevaluation = new HashSet<(uint CountryId, int adminLevel)>();
        foreach (var (id, _) in relevantThings)
        {
            if (oldState.AdminsToCountry.TryGetValue(id, out var relevantLevels))
            {
                foreach (var countryLevel in relevantLevels)
                {
                    thingsThatNeedReevaluation.Add(countryLevel);
                }
            }
        }
        var countries = oldState.Countries.ToDictionary(d => d.RelationId);
        foreach (var relevantThing in relevantThings)
        {
            if (countries.TryGetValue(relevantThing.Id, out var country))
            {
                foreach (var level in country.Admins)
                {
                    thingsThatNeedReevaluation.Add((country.RelationId, int.Parse(level.Key)));
                }
            }
        }

        foreach (var (CountryId, adminLevel) in database.GetCountryAndLevelForAdmins(relevantThings.Select((i) => i.Id).Except(countries.Keys).ToList(), countries.Keys.ToList()))
        {
            thingsThatNeedReevaluation.Add(((uint)CountryId, adminLevel));
        }

        var newState = new StateOfTheAdmins(oldState.AdminsToCountry);
        foreach (var oldCountry in oldState.Countries)
        {
            var currentCountry = new Country() {
                EnglishName = oldCountry.EnglishName,
                Iso2 = oldCountry.Iso2,
                Iso3 = oldCountry.Iso3,
                RelationId = oldCountry.RelationId
            };
            newState.Countries.Add(currentCountry);
            if (!database.DoesCountryExist(oldCountry.RelationId))
            {
                foreach (var expectedAdminLevelGroup in oldCountry.Admins)
                {
                    currentCountry.Admins.Add(expectedAdminLevelGroup.Key, new(0));
                }
                currentCountry.IsValid = false;
                continue;
            }
            currentCountry.IsValid = true;
            foreach (var expectedAdminLevelGroup in oldCountry.Admins)
            {
                int adminLevel = int.Parse(expectedAdminLevelGroup.Key);

                if (thingsThatNeedReevaluation.Contains((currentCountry.RelationId, adminLevel)))
                {
                    var actualAdmins = database.GetCountryAdmins(oldCountry.RelationId, adminLevel);
                    actualAdmins.Sort();
                    currentCountry.Admins.Add(expectedAdminLevelGroup.Key, actualAdmins);
                    foreach (var item in actualAdmins)
                    {
                        if (!newState.AdminsToCountry.TryGetValue((uint)item, out var memberOf))
                        {
                            newState.AdminsToCountry[(uint)item] = memberOf = new();
                        }
                        memberOf.Add((currentCountry.RelationId, adminLevel));
                    }
                }
                else
                {
                    currentCountry.Admins.Add(expectedAdminLevelGroup.Key, expectedAdminLevelGroup.Value);
                }
            }
        }
        return newState;
    }

    private StateOfTheAdmins CreateCurrentState(StateOfTheAdmins expectedState)
    {
        var newState = new StateOfTheAdmins();
        foreach (var expectedCountry in expectedState.Countries)
        {
            var currentCountry = new Country() {
                EnglishName = expectedCountry.EnglishName,
                Iso2 = expectedCountry.Iso2,
                Iso3 = expectedCountry.Iso3,
                RelationId = expectedCountry.RelationId
            };
            newState.Countries.Add(currentCountry);
            currentCountry.IsValid = database.DoesCountryExist(expectedCountry.RelationId);
            foreach (var expectedAdminLevelGroup in expectedCountry.Admins)
            {
                int adminLevel = int.Parse(expectedAdminLevelGroup.Key);
                var actualAdmins = currentCountry.IsValid ? database.GetCountryAdmins(expectedCountry.RelationId, adminLevel) : new(0);
                currentCountry.Admins.Add(expectedAdminLevelGroup.Key, actualAdmins);
                actualAdmins.Sort();
                foreach (var item in actualAdmins)
                {
                    if (!newState.AdminsToCountry.TryGetValue((uint)item, out var memberOf))
                    {
                        newState.AdminsToCountry[(uint)item] = memberOf = new();
                    }
                    memberOf.Add((currentCountry.RelationId, adminLevel));
                }
            }
        }
        return newState;
    }
}
