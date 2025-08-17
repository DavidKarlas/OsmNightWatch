using MonoTorrent.Client;
using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using NetTopologySuite.IO;
using NetTopologySuite.Operation.Buffer;
using NetTopologySuite.Operation.Valid;
using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using static OsmNightWatch.Utils;

namespace OsmNightWatch.Analyzers.BrokenCoastline
{
    public class BrokenCoastlineAnalyzer : IOsmAnalyzer
    {
        public string AnalyzerName => "BrokenCoastLine";

        private KeyValueDatabase database;
        private SqliteConnection sqlConnection;
        private Dictionary<uint, (uint wayId, long firstNode, long lastNode)>? allCoastlineWays;

        public BrokenCoastlineAnalyzer(KeyValueDatabase database, string storePath)
        {
            this.database = database;
            sqlConnection = CreateConnection(storePath);
        }

        private SqliteConnection CreateConnection(string storePath)
        {
            var sqlConnection = new SqliteConnection($"Data Source={Path.Combine(storePath, "coastlineSqlite.db")}");

            sqlConnection.Open();
            sqlConnection.EnableExtensions();
            //using (var enable = sqlConnection.CreateCommand()) { enable.CommandText = "PRAGMA enable_load_extension = 1;"; enable.ExecuteNonQuery(); }
            using (var load = sqlConnection.CreateCommand()) { load.CommandText = "SELECT load_extension('mod_spatialite')"; load.ExecuteNonQuery(); }
            var existsCommand = sqlConnection.CreateCommand("SELECT name FROM sqlite_master WHERE type='table' AND name='Coastlines';");
            if (existsCommand.ExecuteScalar() == null)
            {
                using var transaction = sqlConnection.BeginTransaction();
                sqlConnection.ExecuteNonQuery("SELECT InitSpatialMetaData();");
                sqlConnection.ExecuteNonQuery("CREATE TABLE Coastlines (Id int PRIMARY KEY, FirstNode int, LastNode int, Reason text NULL);");
                sqlConnection.ExecuteNonQuery("SELECT AddGeometryColumn('Coastlines', 'geom',  4326, 'GEOMETRY', 'XY');");
                sqlConnection.ExecuteNonQuery("SELECT CreateSpatialIndex('Coastlines', 'geom');");
                transaction.Commit();
            }
            return sqlConnection;
        }

        public FilterSettings FilterSettings { get; } = new FilterSettings() {
            Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Way,
                    new[] { new TagFilter("natural", "coastline") },
                    needsWays: false,
                    needsNodes:true)
            }
        };

        public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
        {
            Log("Starting Batch Load");
            Utils.BatchLoad(relevantThings, newOsmSource, true, true);
            Log("Finished Batch Load");
            allCoastlineWays = new(1_500_000);
            var list = new List<(uint WayId, Geometry Geometry)>(1_500_000);
            database.RelationChangesTracker.AddWaysToTrack(relevantThings.OfType<Way>());
            Log("Starting Way to Sqlite conversion");
            using (var transaction = sqlConnection.BeginTransaction())
            {
                foreach (Way way in relevantThings)
                {
                    var (_, geometry, _) = UpsertCoastline(way, newOsmSource);
                    if (geometry != null)
                        list.Add(((uint)way.Id, geometry));
                }
                UpdateCrossesWithReason(list, new());
                transaction.Commit();
            }
            Log("Finished Way to Sqlite conversion");
            return ProcessAllWays();
        }

        private void UpdateCrossesWithReason(List<(uint WayId, Geometry Geometry)> list, HashSet<uint> allCoastlinesWithCrossesReasonInDatabase)
        {
            var tree = new STRtree<(uint WayId, Geometry Geometry)>();
            foreach (var item in list)
            {
                tree.Insert(item.Geometry.EnvelopeInternal, item);
            }
            tree.Build();
            foreach (var item in list)
            {
                foreach (var candidate in tree.Query(item.Geometry.EnvelopeInternal))
                {
                    if (candidate.WayId == item.WayId)
                        continue;
                    var matrix = candidate.Geometry.Relate(item.Geometry);
                    if (matrix.IsCrosses(Dimension.Curve, Dimension.Curve) || matrix.IsOverlaps(Dimension.Curve, Dimension.Curve))
                    {
                        UpdateCoastlineReason(candidate.WayId, $"Crosses with {item.WayId}");
                        UpdateCoastlineReason(item.WayId, $"Crosses with {candidate.WayId}");
                        allCoastlinesWithCrossesReasonInDatabase.Remove(candidate.WayId);
                        allCoastlinesWithCrossesReasonInDatabase.Remove(item.WayId);
                    }
                }
            }

            foreach (var coastlinesThatUsedToBeCrossesWith in allCoastlinesWithCrossesReasonInDatabase)
            {
                UpdateCoastlineReason(coastlinesThatUsedToBeCrossesWith, null);
            }
        }

        private HashSet<uint> QueryAllCoastlinesWithCrossesReasonInDatabase()
        {
            var problematicCoastlines = new HashSet<uint>();
            using (var comm = sqlConnection.CreateCommand("SELECT id, reason FROM coastlines WHERE reason IS NOT NULL AND geom IS NOT NULL"))
            {
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        if (!reader.GetString(1).StartsWith("Crosses with "))
                        {
                            throw new InvalidOperationException("There was assumption that only 'Crosses with' are valid reasons WHEN geometry is not null, what changed??!?!");
                        }
                        problematicCoastlines.Add((uint)reader.GetInt32(0));
                    }
                    return problematicCoastlines;
                }
            }
        }

        private void UpdateCoastlineReason(uint wayId, string? reason)
        {
            using (var comm = sqlConnection.CreateCommand($"UPDATE Coastlines SET reason = @reason WHERE id = @wayId"))
            {
                comm.Parameters.AddWithValue("wayId", wayId);
                comm.Parameters.AddWithValue("reason", reason ?? (object)DBNull.Value);
                comm.ExecuteNonQuery();
            }
        }

        private List<(uint WayId, Geometry Geometry)> QueryCoastlinesAndNeigbours(HashSet<uint> wayId)
        {
            var list = new List<(uint WayId, Geometry Geometry)>(wayId.Count * 2);
            byte[] buffer = new byte[64 * 1024];
            var gaiaReader = new GaiaGeoReader();
            using (var comm = sqlConnection.CreateCommand(@$"SELECT searchedCoastline.id, searchedCoastline.geom FROM Coastlines searchedCoastline, Coastlines modifiedCoastline WHERE 
                                                           modifiedCoastline.id IN ({string.Join(",", wayId)}) AND
                                                           searchedCoastline.rowid IN (SELECT ROWID FROM SpatialIndex WHERE f_table_name='Coastlines' AND search_frame=modifiedCoastline.geom);"))
            {
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        if (reader.IsDBNull(1))
                            continue; //throw new InvalidOperationException("Why is this here? How can geometry be null but still find spatial index match?");
                        var read = reader.GetBytes(1, 0, buffer, 0, buffer.Length);
                        if (read == buffer.Length)
                            throw new Exception("Too big byte array for buffer! How? I thought 2k nodes per way limit * (lat+lon) would max at 16kb.");
                        var buffer2 = new byte[read];
                        Array.Copy(buffer, buffer2, read);
                        var coastlineGeometry = gaiaReader.Read(buffer2);
                        list.Add(((uint)reader.GetInt32(0), coastlineGeometry));
                    }
                    return list;
                }
            }
        }

        static GaiaGeoWriter gaiaWriter = new GaiaGeoWriter();

        private (Way Way, Geometry? Geometry, string? issue) UpsertCoastline(Way way, IOsmGeoBatchSource newOsmSource)
        {
            string? reason;
            Geometry? geometry;
            if (way.Nodes.Length < 2)
            {
                geometry = null;
                reason = "Way has less than 2 nodes.";
            }
            else
            {
                var array = new Coordinate[way.Nodes.Length];
                for (int i = 0; i < way.Nodes.Length; i++)
                {
                    var node = newOsmSource.GetNode(way.Nodes[i]);
                    array[i] = node.ToCoordinate();
                }
                if (way.Nodes[0] == way.Nodes[^1])
                {
                    var lineRing = new LinearRing(array.ToArray()) {
                        SRID = 4326
                    };
                    var polygon = new Polygon(lineRing) {
                        SRID = 4326
                    };
                    if (!polygon.Shell.IsCCW)
                    {
                        reason = "Coastline representing island should be counterclockwise.";
                    }
                    else
                    {
                        var validate = new IsValidOp(polygon);
                        reason = validate.IsValid ? null : validate.ValidationError.ToString();
                    }
                    geometry = lineRing;
                }
                else
                {
                    geometry = new LineString(array.ToArray()) {
                        SRID = 4326
                    };
                    var validate = new IsValidOp(geometry);
                    reason = validate.IsValid ? null : validate.ValidationError.ToString();
                    if (reason == null && !geometry.IsSimple)
                    {
                        var simpleOp = new IsSimpleOp(geometry);
                        reason = "Coastline is self-intersecting at " + simpleOp.NonSimpleLocation;
                    }
                }
                allCoastlineWays![(uint)way.Id] = ((uint)way.Id, way.Nodes[0], way.Nodes[^1]);
            }

            // There should be only one possibility for geometry != null and reason set in database
            // and that is if valid geometry intersects with another geometry
            if (reason != null)
            {
                geometry = null;
            }

            using (var comm = sqlConnection.CreateCommand("INSERT INTO Coastlines (Id, firstNode, lastNode, geom, reason) VALUES (@id, @firstNode, @lastNode, @geom, @reason) ON CONFLICT (Id) DO UPDATE SET geom = @geom, firstNode = @firstNode, lastNode = @lastNode, Reason = @reason"))
            {
                comm.Parameters.AddWithValue("id", way.Id);
                comm.Parameters.AddWithValue("firstNode", way.Nodes[0]);
                comm.Parameters.AddWithValue("lastNode", way.Nodes[^1]);
                comm.Parameters.AddWithValue("geom", geometry == null ? DBNull.Value : gaiaWriter.Write(geometry));
                comm.Parameters.AddWithValue("reason", (object?)reason ?? DBNull.Value);
                comm.ExecuteNonQuery();
            }
            return (way, geometry, reason);
        }

        private void DeleteCoastline(long wayId)
        {
            sqlConnection.ExecuteNonQuery($"DELETE FROM Coastlines WHERE Id = {wayId}");
        }

        public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoBatchSource newOsmSource)
        {
            if (allCoastlineWays == null)
            {
                allCoastlineWays = LoadAllCoastline();
            }
            var coastlinesToDelete = new HashSet<uint>();
            var coastlinesToUpsert = new List<Way>();
            var waysToCheck = new HashSet<uint>();
            var waysModifiedAndModifiedByNodesMovement = database.RelationChangesTracker.GetChangedWays(changeSet);
            // Drop modifiedWays since those will be checked later anyway
            foreach (var wayId in changeSet.Ways.Keys)
            {
                waysModifiedAndModifiedByNodesMovement.Remove(wayId);
            }
            // Drop anything that is not coastline...
            foreach (var wayId in waysModifiedAndModifiedByNodesMovement.ToArray())
            {
                if (!allCoastlineWays.ContainsKey(wayId))
                    waysModifiedAndModifiedByNodesMovement.Remove(wayId);
            }
            foreach (var newOrModifiedWay in changeSet.Ways
                .Concat(waysModifiedAndModifiedByNodesMovement.Select(wayId => new KeyValuePair<uint, Way?>(wayId, newOsmSource.GetWay(wayId)))))
            {
                if (newOrModifiedWay.Value == null)
                {
                    if (allCoastlineWays.Remove(newOrModifiedWay.Key))
                    {
                        coastlinesToDelete.Add(newOrModifiedWay.Key);
                        waysToCheck.Add(newOrModifiedWay.Key);
                    }
                }
                else if ((newOrModifiedWay.Value.Tags?.TryGetValue("natural", out var naturalValue) ?? false) && naturalValue == "coastline")
                {
                    coastlinesToUpsert.Add(newOrModifiedWay.Value);
                    waysToCheck.Add(newOrModifiedWay.Key);
                }
                else
                {
                    if (allCoastlineWays.Remove(newOrModifiedWay.Key))
                    {
                        coastlinesToDelete.Add(newOrModifiedWay.Key);
                        waysToCheck.Add(newOrModifiedWay.Key);
                    }
                }
            }
            database.RelationChangesTracker.AddWaysToTrack(coastlinesToUpsert);
            Utils.BatchLoad(coastlinesToUpsert, newOsmSource, true, true);
            using (var transaction = sqlConnection.BeginTransaction())
            {
                foreach (var wayId in coastlinesToDelete)
                {
                    DeleteCoastline(wayId);
                }
                foreach (var coastline in coastlinesToUpsert)
                {
                    UpsertCoastline(coastline, newOsmSource);
                }
                var allCoastlinesWithCrossesReasonInDatabase = QueryAllCoastlinesWithCrossesReasonInDatabase();
                waysToCheck.UnionWith(allCoastlinesWithCrossesReasonInDatabase);
                var list = QueryCoastlinesAndNeigbours(waysToCheck);
                UpdateCrossesWithReason(list, allCoastlinesWithCrossesReasonInDatabase);
                transaction.Commit();
            }
            return ProcessAllWays();
        }

        public IEnumerable<IssueData> GetProblematicCoastlines()
        {
            using (var comm = sqlConnection.CreateCommand("SELECT id, reason FROM coastlines WHERE reason IS NOT NULL"))
            {
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        yield return new IssueData() {
                            IssueType = AnalyzerName,
                            OsmType = "W",
                            OsmId = reader.GetInt64(0),
                            Details = reader.GetString(1)
                        };
                    }
                }
            }
        }

        private Dictionary<uint, (uint wayId, long firstNode, long lastNode)> LoadAllCoastline()
        {
            allCoastlineWays = new();
            using (var comm = sqlConnection.CreateCommand())
            {
                comm.CommandText = $"SELECT coastlines.id, coastlines.firstNode, coastlines.lastNode FROM coastlines";
                using var reader = comm.ExecuteReader();
                {
                    while (reader.Read())
                    {
                        var wayId = reader.GetInt64(0);
                        var firstNode = reader.GetInt64(1);
                        var lastNode = reader.GetInt64(2);
                        allCoastlineWays.Add((uint)wayId, ((uint)wayId, firstNode, lastNode));
                    }
                }
            }
            return allCoastlineWays;
        }

        private IEnumerable<IssueData> ProcessAllWays()
        {
            Log("Starting CoastlineValidationTest");
            var (issuesNodes, issuesWays) = new CoastlineValidationTest().Visit(allCoastlineWays!.Values);
            if (issuesWays.Count > 0)
            {
                foreach (var (wayId, details) in issuesWays)
                {
                    yield return new IssueData() {
                        IssueType = AnalyzerName,
                        OsmType = "W",
                        OsmId = wayId,
                        Details = details
                    };
                }
            }

            if (issuesNodes.Count > 0)
            {
                foreach (var (nodeId, details) in issuesNodes)
                {
                    yield return new IssueData() {
                        IssueType = AnalyzerName,
                        OsmType = "N",
                        OsmId = nodeId,
                        Details = details
                    };
                }
            }

            foreach (var issue in GetProblematicCoastlines())
            {
                yield return issue;
            }
        }
    }
}
