using OsmNightWatch.Lib;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch.Analyzers.OpenPolygon {
    public class AdminOpenPolygonAnalyzer : IOsmAnalyzer {
        public string AnalyzerName => "OpenAdminPolygon";

        private bool? IsRelationValid(Relation relation, IOsmGeoSource osmSource, out bool fineWhenIgnoringOuter, out int adminLevel, out string friendlyName) {
            fineWhenIgnoringOuter = false;
            adminLevel = -1;
            if (!relation.Tags.TryGetValue("name:en", out friendlyName))
            {
                if (!relation.Tags.TryGetValue("name", out friendlyName))
                {
                    friendlyName = "";
                }
            }
            if (relation.Tags.TryGetValue("admin_level", out var lvl))
            {
                //If failing to parse...
                if (!double.TryParse(lvl, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
                    return null;
                if (parsed > 8)
                    return null;
                adminLevel = (int)parsed;
            }
            else
            {
                return null;
            }
            if (IsValid(relation, osmSource, false))
            {
                return true;
            }
            if (IsValid(relation, osmSource, true))
            {
                fineWhenIgnoringOuter = true;
            }
            return false;
        }

        public FilterSettings FilterSettings { get; } = new FilterSettings() {
            Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Relation, new[] {
                    new TagFilter("boundary", "administrative"),
                    new TagFilter("type", "boundary"),
                    new TagFilter("admin_level", "2", "3","4","5","6", "7", "8")
                }, true,false)
            }
        };

        public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource osmSource) {
            Utils.BatchLoad(relevantThings, osmSource, true, false);

            foreach (var relevantThing in relevantThings)
            {
                if (relevantThing is Relation relation)
                {
                    if (IsRelationValid(relation, osmSource, out var fineWhenIgnoringMemberType, out var adminLevel, out var friendlyName) ?? true)
                        continue;
                    var issueType = AnalyzerName;
                    if (adminLevel > 6)
                    {
                        issueType += adminLevel;
                    }
                    yield return new IssueData() {
                        IssueType = issueType,
                        OsmType = "R",
                        FriendlyName = friendlyName,
                        OsmId = relation.Id!.Value,
                        Details = fineWhenIgnoringMemberType ? "Member missing 'inner' or 'outer' member role." : "Disconnected relation."
                    };
                }
            }
        }

        public static bool IsValid(Relation r, IOsmGeoSource db, bool ignoreMemberRole) {
            var hashSet = new HashSet<long>();

            foreach (var way in r.Members.Where(m => m.Type == OsmGeoType.Way && (ignoreMemberRole ? true : (m.Role == "inner" || m.Role == "outer"))).Select(m => db.GetWay(m.Id)))
            {
                var nodes = way.Nodes;
                long first = nodes[0];
                long last = nodes[nodes.Length - 1];

                if (!hashSet.Remove(first))
                {
                    hashSet.Add(first);
                }
                if (!hashSet.Remove(last))
                {
                    hashSet.Add(last);
                }
            }

            if (hashSet.Count > 0)
            {
                return false;
            }
            return true;
        }
    }
}
