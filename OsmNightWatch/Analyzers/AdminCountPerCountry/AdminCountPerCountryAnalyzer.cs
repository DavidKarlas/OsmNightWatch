using NetTopologySuite.Geometries.Prepared;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
using System.Text.Json;
using NetTopologySuite.Index.Strtree;
using System.Net.Http.Headers;
using static OsmNightWatch.Utils;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public partial class AdminCountPerCountryAnalyzer : IOsmAnalyzer, IDisposable
{
    SqliteConnection sqlConnection;
    private KeyValueDatabase database;
    private HttpClient httpClient = new HttpClient();
    public AdminCountPerCountryAnalyzer(KeyValueDatabase database, string storePath)
    {
        this.database = database;
        sqlConnection = new SqliteConnection($"Data Source={Path.Combine(storePath, "sqlite.db")}");
        Initialize();
    }

    public void Initialize()
    {
        sqlConnection.Open();
        Console.WriteLine("H1");
        sqlConnection.EnableExtensions();
        Console.WriteLine("H2");
        using (var enable = sqlConnection.CreateCommand()) { enable.CommandText = "PRAGMA enable_load_extension = 1;"; enable.ExecuteNonQuery(); }
        Console.WriteLine("H3");
        using (var load = sqlConnection.CreateCommand()) { load.CommandText = "SELECT load_extension('mod_spatialite')"; load.ExecuteNonQuery(); }
        Console.WriteLine("H4");
        using var existsCommand = sqlConnection.CreateCommand("SELECT name FROM sqlite_master WHERE type='table' AND name='Admins';");
        Console.WriteLine("H5");
        if (existsCommand.ExecuteScalar() == null)
        {
            Console.WriteLine("H6");
            using var transaction = sqlConnection.BeginTransaction();
            sqlConnection.ExecuteNonQuery("SELECT InitSpatialMetaData();");
            sqlConnection.ExecuteNonQuery("CREATE TABLE Admins (Id int PRIMARY KEY, FriendlyName text, AdminLevel int, Reason text NULL);");
            sqlConnection.ExecuteNonQuery("SELECT AddGeometryColumn('Admins', 'geom',  4326, 'GEOMETRY', 'XY');");
            sqlConnection.ExecuteNonQuery("SELECT CreateSpatialIndex('Admins', 'geom');");
            transaction.Commit();
        }
    }

    public void DeleteAdmin(long id)
    {
        lock (sqlConnection)
        {
            sqlConnection.ExecuteNonQuery($"DELETE FROM Admins WHERE Id = {id}");
        }
    }

    public void EmptyUpsertAdmins(object param)
    {
        var (adminsToUpsert, adminsToUpsertAbortOrCommitTask) = ((BlockingCollection<(long id, string friendlyName, int adminLevel, byte[]? polygon, string? reason)>, TaskCompletionSource<bool>))param;
        var transaction = sqlConnection.BeginTransaction();
        (long id, string friendlyName, int adminLevel, byte[]? polygon, string? reason) blob;
        while (!adminsToUpsert.IsCompleted)
        {
            if (!adminsToUpsert.TryTake(out blob, 100))
            {
                continue;
            }
            var (id, friendlyName, adminLevel, polygon, reason) = blob;
            using (var comm = sqlConnection.CreateCommand("INSERT INTO Admins (Id, FriendlyName, AdminLevel, geom, reason) VALUES (@id, @friendlyName, @adminLevel, @geom, @reason) ON CONFLICT (Id) DO UPDATE SET geom = @geom, FriendlyName = @friendlyName, AdminLevel = @adminLevel, Reason = @reason"))
            {
                comm.Parameters.AddWithValue("id", id);
                comm.Parameters.AddWithValue("friendlyName", friendlyName);
                comm.Parameters.AddWithValue("adminLevel", adminLevel);
                if (polygon != null)
                {
                    comm.Parameters.AddWithValue("geom", polygon);
                }
                else
                {
                    comm.Parameters.AddWithValue("geom", DBNull.Value);
                }
                comm.Parameters.AddWithValue("reason", reason ?? (object)DBNull.Value);
                comm.ExecuteNonQuery();
            }
        }
        var commitTransaction = adminsToUpsertAbortOrCommitTask.Task.Result;
        if (commitTransaction)
        {
            transaction.Commit();
        }
        else
        {
            transaction.Dispose();
        }
    }

    public string AnalyzerName => nameof(AdminCountPerCountryAnalyzer);

    public FilterSettings FilterSettings { get; } = new FilterSettings() {
        Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Relation, new[] {
                    new TagFilter("boundary", "administrative"),
                    new TagFilter("type", "boundary"),
                    new TagFilter("admin_level", "2", "3","4","5","6","7","8", "9") },
                    needsWays: true,
                    needsNodes: true)
            }
    };

    private IEnumerable<IssueData> UpdateRelations((uint Id, Relation Relation)[] relevantThings, IOsmGeoSource newOsmSource)
    {
        Log("Starting admins upsert");
        var expectedState = GetExpectedState();
        var relationsToCheckAdminCenter = new HashSet<uint>(expectedState.Countries.SelectMany(c => c.Admins.OrderBy(a => a.Key).FirstOrDefault().Value ?? []));
        expectedState.Countries.ForEach(c => relationsToCheckAdminCenter.Add(c.RelationId));

        StartAdminsUpsertTransaction();
        try
        {
            Parallel.ForEach(relevantThings, new ParallelOptions() {
                MaxDegreeOfParallelism = 32
            }, (admin) => {
                // Drop admin from database if it was deleted or doesn't meet admin criteria
                if (admin.Relation == null || admin.Relation.Tags == null || !FilterSettings.Filters.Single().Matches(admin.Relation))
                {
                    DeleteAdmin(admin.Id);
                    return;
                }
                var result = BuildPolygonFromRelation.BuildPolygon(admin.Relation, newOsmSource);
                database.RelationChangesTracker.AddRelationToTrack(admin.Id, result.Ways, admin.Relation.Members.Where(m => m.Type == OsmGeoType.Node).Select(m => m.Id));
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

                if (string.IsNullOrEmpty(result.reason) && relationsToCheckAdminCenter.Contains(admin.Id) && CheckAdminCentre(admin.Relation, newOsmSource) is string adminCentreReason)
                {
                    result.reason = adminCentreReason;
                }

                if (!admin.Relation.Tags.TryGetValue("name:en", out var friendlyName))
                {
                    if (!admin.Relation.Tags.TryGetValue("name", out friendlyName))
                    {
                        friendlyName = "";
                    }
                }
                UpsertAdmin(admin.Id, friendlyName, int.Parse(admin.Relation.Tags!["admin_level"]), geom, result.reason);
            });
        }
        catch
        {
            EndAdminsUpsertTransaction(false);
            throw;
        }
        EndAdminsUpsertTransaction(true);

        Log("Finished admins upsert");
        if (currentState == null)
        {
            currentState = CreateCurrentState(expectedState!);
        }
        else
        {
            currentState = UpdateCurrentState(currentState, relevantThings);
        }

        foreach (var (relationId, name, adminLevel, reason) in GetAdminsWithReason())
        {
            if (reason.Contains("admin_centre"))
            {
                yield return new IssueData() {
                    IssueType = "AdminCentre",
                    FriendlyName = name,
                    OsmType = "R",
                    OsmId = relationId,
                    Details = reason
                };
            }
        }

        foreach (var (relationId, name, adminLevel, reason) in GetBrokenAdmins())
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
        Log("Finished comparing admins");
    }

    private string? CheckAdminCentre(Relation relation, IOsmGeoSource newOsmSource)
    {
        var adminCentreCount = relation.Members.Count(m => m.Role == "admin_centre");
        if (adminCentreCount == 0)
        {
            return "Missing admin_centre role";
        }
        if (adminCentreCount > 1)
        {
            return "Too many admin_centre role roles";
        }
        var member = relation.Members.First(m => m.Role == "admin_centre");
        if (member.Type != OsmGeoType.Node)
        {
            return "admin_centre member is not Node";
        }
        var node = newOsmSource.GetNode(member.Id);
        if (!(node.Tags?.TryGetValue("place", out var placeValue) ?? false))
            return $"admin_centre node({member.Id}) does not have 'place' tag.";
        //switch (placeValue)
        //{
        //    case "town":
        //    case "village":
        //    case "city":
        //    case "suburb":
        //        return null;

        //    default:
        //        return $"admin_centre node has place value set to {placeValue}, but must be 'village', 'town' or 'city'.";
        //}
        return null;
    }

    private StateOfTheAdmins? cachedExpectedState = null;
    private EntityTagHeaderValue? latestEtag;

    private StateOfTheAdmins GetExpectedState()
    {
        var httpRequest = new HttpRequestMessage(HttpMethod.Get, "https://github.com/DavidKarlas/OsmNightWatch/releases/latest/download/ExpectedStateOfAdmins.json");
        if (latestEtag != null)
            httpRequest.Headers.IfNoneMatch.Add(latestEtag);
        var responseMessage = httpClient.Send(httpRequest);
        if (cachedExpectedState != null && responseMessage.StatusCode == System.Net.HttpStatusCode.NotModified)
            return cachedExpectedState;
        var expectedStateJson = responseMessage.Content.ReadAsStringAsync().Result;
        var expectedState = new StateOfTheAdmins() {
            Countries = JsonSerializer.Deserialize<List<Country>>(expectedStateJson, new JsonSerializerOptions() {
                ReadCommentHandling = JsonCommentHandling.Skip
            })!
        };
        latestEtag = responseMessage.Headers.ETag;
        cachedExpectedState = expectedState;
        return expectedState;
    }

    public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
    {
        Utils.BatchLoad(relevantThings, newOsmSource, true, true);
        return UpdateRelations(relevantThings.Select(r => ((uint)r.Id, (Relation)r)).ToArray(), newOsmSource);
    }

    StateOfTheAdmins? currentState;

    public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoBatchSource newOsmSource)
    {
        var filter = FilterSettings.Filters.Single();

        var changedRelations = database.RelationChangesTracker.GetChangedRelations(changeSet);
        foreach (var relation in changeSet.Relations.Values)
        {
            if (relation == null)
                continue;
            if (filter.Matches(relation))
            {
                changedRelations.Add((uint)relation.Id);
            }
        }

        var relevantThings = newOsmSource.BatchLoad(relationIds: new HashSet<long>(changedRelations.Select(r => (long)r))).relations;
        Utils.BatchLoad(relevantThings, newOsmSource, true, true);
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

        foreach (var (CountryId, adminLevel) in GetCountryAndLevelForAdmins(relevantThings.Select((i) => i.Id).Except(countries.Keys).ToList(), countries.Keys.ToList()))
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
            if (!DoesCountryExist(oldCountry.RelationId))
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
                    var actualAdmins = GetCountryAdmins(oldCountry.RelationId, adminLevel);
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

    public StateOfTheAdmins CreateCurrentState(StateOfTheAdmins expectedState)
    {
        var newState = new StateOfTheAdmins();
        Parallel.ForEach(expectedState.Countries, new ParallelOptions() { MaxDegreeOfParallelism = 16 }, (expectedCountry) => {
            var currentCountry = new Country() {
                EnglishName = expectedCountry.EnglishName,
                Iso2 = expectedCountry.Iso2,
                Iso3 = expectedCountry.Iso3,
                RelationId = expectedCountry.RelationId
            };
            lock (newState)
            {
                newState.Countries.Add(currentCountry);
            }
            currentCountry.IsValid = DoesCountryExist(expectedCountry.RelationId);
            foreach (var expectedAdminLevelGroup in expectedCountry.Admins)
            {
                int adminLevel = int.Parse(expectedAdminLevelGroup.Key);
                var actualAdmins = currentCountry.IsValid ? GetCountryAdmins(expectedCountry.RelationId, adminLevel) : new(0);
                currentCountry.Admins.Add(expectedAdminLevelGroup.Key, actualAdmins);
                actualAdmins.Sort();
                foreach (var item in actualAdmins)
                {
                    var memberOf = newState.AdminsToCountry.GetOrAdd((uint)item, (_) => new());
                    lock (memberOf)
                    {
                        memberOf.Add((currentCountry.RelationId, adminLevel));
                    }
                }
            }
        });
        return newState;
    }

    public void Dispose()
    {
        sqlConnection.Close();
    }

    public List<(long CountryId, int adminLevel)> GetCountryAndLevelForAdmins(List<uint> relevantAdmins, List<uint> allCountries)
    {
        if (relevantAdmins.Count == 0)
            return new List<(long, int)>();

        byte[] buffer = new byte[15 * 1024 * 1024];
        var gaiaReader = new GaiaGeoReader();
        STRtree<(long CountryId, PreparedPolygon Polygon)> tree = new STRtree<(long CountryId, PreparedPolygon Polygon)>();
        using (var comm = sqlConnection.CreateCommand())
        {
            comm.CommandText = $"SELECT adm.id, adm.geom FROM admins adm WHERE adm.id in ({string.Join(",", allCountries)});";
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    if (reader.IsDBNull(1))
                        continue;

                    var read = reader.GetBytes(1, 0, buffer, 0, buffer.Length);
                    if (read == buffer.Length)
                        throw new Exception("Too big byte array for buffer!");
                    var buffer2 = new byte[read];
                    Array.Copy(buffer, buffer2, read);
                    var polygon = new PreparedPolygon((IPolygonal)gaiaReader.Read(buffer2));
                    tree.Insert(polygon.Geometry.EnvelopeInternal, (reader.GetInt64(0), polygon));
                }
            }
        }

        var result = new HashSet<(long CountryId, int adminLevel)>();
        using (var comm = sqlConnection.CreateCommand($"SELECT adm.adminLevel, adm.geom FROM admins adm WHERE adm.id in ({string.Join(",", relevantAdmins)})"))
        {
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    if (reader.IsDBNull(1))
                        continue;

                    var read = reader.GetBytes(1, 0, buffer, 0, buffer.Length);
                    if (read == buffer.Length)
                        throw new Exception("Too big byte array for buffer!");
                    var buffer2 = new byte[read];
                    Array.Copy(buffer, buffer2, read);
                    var adminGeometry = gaiaReader.Read(buffer2);
                    foreach (var (countryId, polygon) in tree.Query(adminGeometry.EnvelopeInternal))
                    {
                        if (polygon.Intersects(adminGeometry))
                        {
                            if (polygon.Contains(adminGeometry))
                            {
                                result.Add((countryId, reader.GetInt32(0)));
                            }
                            else if (polygon.Overlaps(adminGeometry))
                            {
                                result.Add((countryId, reader.GetInt32(0)));
                            }
                        }
                    }
                }
            }
        }
        return result.ToList();
    }

    BlockingCollection<(long id, string friendlyName, int adminLevel, byte[]? polygon, string? reason)>? AdminsToUpsert;
    TaskCompletionSource<bool> AdminsToUpsertAbortOrCommitTask;

    public void StartAdminsUpsertTransaction()
    {
        if (AdminsToUpsert != null)
        {
            throw new Exception();
        }
        var adminsToUpsert = AdminsToUpsert = new(128);
        var adminsToUpsertAbortOrCommitTask = AdminsToUpsertAbortOrCommitTask = new TaskCompletionSource<bool>();
        Task.Run(() => {
            EmptyUpsertAdmins((adminsToUpsert, adminsToUpsertAbortOrCommitTask));
        });
    }

    public void EndAdminsUpsertTransaction(bool commitTransaction)
    {
        if (AdminsToUpsert == null)
        {
            throw new Exception();
        }
        AdminsToUpsert.CompleteAdding();
        AdminsToUpsert = null;
        AdminsToUpsertAbortOrCommitTask.SetResult(commitTransaction);
        AdminsToUpsertAbortOrCommitTask = null;
    }

    static GaiaGeoWriter gaiaWriter = new GaiaGeoWriter();

    public void UpsertAdmin(long id, string friendlyName, int adminLevel, Geometry? polygon, string? reason)
    {
        if (AdminsToUpsert == null)
        {
            throw new Exception();
        }
        byte[]? data = null;
        if (polygon != null)
        {
            polygon.SRID = 4326;
            data = gaiaWriter.Write(polygon);
        }
        AdminsToUpsert.Add((id, friendlyName, adminLevel, data, reason));
    }

    public IEnumerable<(long RelationId, string name, int adminLevel, string reason)> GetBrokenAdmins()
    {
        using (var comm = sqlConnection.CreateCommand("SELECT id, friendlyname, adminlevel, reason FROM admins WHERE geom IS NULL"))
        {
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    yield return (reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2), reader.GetString(3));
                }
            }
        }
    }
    public IEnumerable<(long RelationId, string name, int adminLevel, string reason)> GetAdminsWithReason()
    {
        using (var comm = sqlConnection.CreateCommand("SELECT id, friendlyname, adminlevel, reason FROM admins WHERE reason IS NOT NULL"))
        {
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    yield return (reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2), reader.GetString(3));
                }
            }
        }
    }

    public bool DoesCountryExist(long relationId)
    {
        using (var comm = sqlConnection.CreateCommand("SELECT id FROM admins WHERE id=@relationId and geom IS NOT NULL"))
        {
            comm.Parameters.AddWithValue("@relationId", relationId);
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    return true;
                }
                return false;
            }
        }
    }

    public List<uint> GetCountryAdmins(long relationId, int adminLevel)
    {
        var result = new List<uint>();
        byte[] buffer = new byte[15 * 1024 * 1024];
        var gaiaReader = new GaiaGeoReader();
        PreparedPolygon polygon = null;
        using (var comm = sqlConnection.CreateCommand())
        {
            comm.CommandText = "SELECT adm.id, adm.geom FROM admins adm WHERE adm.id=@relationId;";
            comm.Parameters.AddWithValue("@relationId", relationId);
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    if (reader.IsDBNull(1))
                        return result;

                    var read = reader.GetBytes(1, 0, buffer, 0, buffer.Length);
                    if (read == buffer.Length)
                        throw new Exception("Too big byte array for buffer!");
                    var buffer2 = new byte[read];
                    Array.Copy(buffer, buffer2, read);
                    polygon = new PreparedPolygon((IPolygonal)gaiaReader.Read(buffer2));
                }
            }
        }

        if (polygon == null)
            return result;

        using (var comm = sqlConnection.CreateCommand(@"SELECT adm.id, adm.geom FROM admins adm, admins country WHERE 
                                                           country.id = @relationId AND
                                                           adm.adminlevel = @adminLevel AND
                                                           adm.rowid IN (SELECT ROWID FROM SpatialIndex WHERE f_table_name='admins' AND search_frame=country.geom);"))
        {
            comm.Parameters.AddWithValue("@relationId", relationId);
            comm.Parameters.AddWithValue("@adminLevel", adminLevel);
            using var reader = comm.ExecuteReader();
            {
                while (reader.Read())
                {
                    if (reader.IsDBNull(1))
                        continue;
                    var read = reader.GetBytes(1, 0, buffer, 0, buffer.Length);
                    if (read == buffer.Length)
                        throw new Exception("Too big byte array for buffer!");
                    var buffer2 = new byte[read];
                    Array.Copy(buffer, buffer2, read);
                    var adminGeometry = gaiaReader.Read(buffer2);
                    if (polygon.Intersects(adminGeometry))
                    {
                        if (polygon.Contains(adminGeometry))
                        {
                            result.Add((uint)reader.GetInt64(0));
                        }
                        else if (polygon.Overlaps(adminGeometry))
                        {
                            result.Add((uint)reader.GetInt64(0));
                        }
                    }
                }
                return result;
            }
        }
    }


}
