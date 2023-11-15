using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch.Analyzers.Waterbodies
{
    public class WaterbodiesAnalyzer : IOsmAnalyzer
    {
        SQLiteConnection sqlConnection;
        private KeyValueDatabase database;

        public WaterbodiesAnalyzer(KeyValueDatabase database, string storePath)
        {
            this.database = database;
            sqlConnection = new SQLiteConnection($"Data Source={Path.Combine(storePath, "waterSqlite.db")};Version=3;");
            Initialize();
        }

        public void Initialize()
        {
            sqlConnection.Open();
            sqlConnection.EnableExtensions(true);
            sqlConnection.LoadExtension("mod_spatialite");
            using var existsCommand = sqlConnection.CreateCommand("SELECT name FROM sqlite_master WHERE type='table' AND name='Waterbodies';");
            if (existsCommand.ExecuteScalar() == null)
            {
                using var transaction = sqlConnection.BeginTransaction();
                sqlConnection.ExecuteNonQuery("SELECT InitSpatialMetaData();");
                sqlConnection.ExecuteNonQuery("CREATE TABLE Waterbodies (Id int PRIMARY KEY, FriendlyName text, Reason text NULL);");
                sqlConnection.ExecuteNonQuery("SELECT AddGeometryColumn('Waterbodies', 'geom',  4326, 'GEOMETRY', 'XY');");
                sqlConnection.ExecuteNonQuery("SELECT CreateSpatialIndex('Waterbodies', 'geom');");
                transaction.Commit();
            }
        }

        public string AnalyzerName => nameof(WaterbodiesAnalyzer);

        public FilterSettings FilterSettings { get; } = new() {
            Filters = new() { new(OsmGeoType.Relation, new TagFilter[] { new TagFilter("water", "lake") }) }
        };

        public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoBatchSource newOsmSource)
        {

        }

        public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
        {
            Utils.BatchLoad(relevantThings, newOsmSource, true, true);
            return UpdateRelations(relevantThings.Select(r => ((uint)r.Id, (Relation)r)).ToArray(), newOsmSource);
        }

        public void DeleteAdmin(long id)
        {
            lock (sqlConnection)
            {
                sqlConnection.ExecuteNonQuery($"DELETE FROM Waterbodies WHERE Id = {id}");
            }
        }

        private IEnumerable<IssueData> UpdateRelations((uint Id, Relation Relation)[] relevantThings, IOsmGeoSource newOsmSource)
        {
            StartWaterbodiesUpsertTransaction();
            try
            {
                Parallel.ForEach(relevantThings, new ParallelOptions() {
                    MaxDegreeOfParallelism = 32
                }, (admin) => {
                    if (admin.Relation == null || admin.Relation.Tags == null || !FilterSettings.Filters.Single().Matches(admin.Relation))
                    {
                        DeleteAdmin(admin.Id);
                        return;
                    }
                    var result = BuildPolygonFromRelation.BuildPolygon(admin.Relation, newOsmSource);
                    database.RelationChangesTracker.AddRelationToTrack(admin.Id, result.Ways);
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
                    UpsertAdmin(admin.Id, friendlyName, geom, result.reason);
                });
            }
            catch
            {
                EndWaterbodiesUpsertTransaction(false);
                throw;
            }
            EndWaterbodiesUpsertTransaction(true);
            foreach (var broken in GetBrokenWaterbodies())
            {
                yield return new IssueData() {
                    FriendlyName = broken.name,
                    Details = broken.reason,
                    OsmType = "R",
                    OsmId = broken.RelationId,
                    IssueType = "Waterbodies"
                };
            }
        }

        public IEnumerable<(long RelationId, string name, string reason)> GetBrokenWaterbodies()
        {
            using (var comm = sqlConnection.CreateCommand("SELECT id, friendlyname, reason FROM waterbodies WHERE geom IS NULL"))
            {
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        yield return (reader.GetInt64(0), reader.GetString(1), reader.GetString(2));
                    }
                }
            }
        }

        public void EndWaterbodiesUpsertTransaction(bool commitTransaction)
        {
            if (WaterbodiesToUpsert == null)
            {
                throw new Exception();
            }
            WaterbodiesToUpsert.CompleteAdding();
            WaterbodiesToUpsert = null;
            WaterbodiesToUpsertAbortOrCommitTask.SetResult(commitTransaction);
            WaterbodiesToUpsertAbortOrCommitTask = null;
        }

        static GaiaGeoWriter gaiaWriter = new GaiaGeoWriter();

        public void UpsertAdmin(long id, string friendlyName, Geometry? polygon, string? reason)
        {
            if (WaterbodiesToUpsert == null)
            {
                throw new Exception();
            }
            byte[]? data = null;
            if (polygon != null)
            {
                polygon.SRID = 4326;
                data = gaiaWriter.Write(polygon);
            }
            WaterbodiesToUpsert.Add((id, friendlyName, data, reason));
        }

        BlockingCollection<(long id, string friendlyName, byte[]? polygon, string? reason)>? WaterbodiesToUpsert;
        TaskCompletionSource<bool> WaterbodiesToUpsertAbortOrCommitTask;

        public void StartWaterbodiesUpsertTransaction()
        {
            if (WaterbodiesToUpsert != null)
            {
                throw new Exception();
            }
            var waterbodiesToUpsert = WaterbodiesToUpsert = new(128);
            var waterbodiesToUpsertAbortOrCommitTask = WaterbodiesToUpsertAbortOrCommitTask = new TaskCompletionSource<bool>();
            Task.Run(() => {
                ProcessCollection(waterbodiesToUpsert, waterbodiesToUpsertAbortOrCommitTask);
            });
        }

        public void ProcessCollection(BlockingCollection<(long id, string friendlyName, byte[]? polygon, string? reason)> waterbodiesToUpsert, TaskCompletionSource<bool> waterbodiesToUpsertAbortOrCommitTask)
        {
            var transaction = sqlConnection.BeginTransaction();
            (long id, string friendlyName, byte[]? polygon, string? reason) blob;
            while (!waterbodiesToUpsert.IsCompleted)
            {
                if (!waterbodiesToUpsert.TryTake(out blob, 100))
                {
                    continue;
                }
                var (id, friendlyName, polygon, reason) = blob;
                using (var comm = sqlConnection.CreateCommand("INSERT INTO Waterbodies (Id, FriendlyName, geom, reason) VALUES (@id, @friendlyName, @geom, @reason) ON CONFLICT (Id) DO UPDATE SET geom = @geom, FriendlyName = @friendlyName, Reason = @reason"))
                {
                    comm.Parameters.AddWithValue("id", id);
                    comm.Parameters.AddWithValue("friendlyName", friendlyName);
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
            var commitTransaction = waterbodiesToUpsertAbortOrCommitTask.Task.Result;
            if (commitTransaction)
            {
                transaction.Commit();
            }
            else
            {
                transaction.Dispose();
            }
        }
    }
}
