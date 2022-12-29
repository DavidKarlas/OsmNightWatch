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

namespace OsmNightWatch.Analyzers.OpenPolygon
{
    public class AdminOpenPolygonAnalyzer : IOsmAnalyzer
    {
        public string AnalyzerName => "OpenAdminPolygon";

        private bool AnalyzeRelation(Relation relation, IOsmGeoSource osmSource)
        {
            if (relation.Tags.TryGetValue("admin_level", out var lvl))
            {
                //If failing to parse...
                if (!double.TryParse(lvl, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
                    return false;
                if (parsed > 7)
                    return false;
            }
            else
            {
                return false;
            }
            if (relation.Tags.TryGetValue("type", out var typeValue) && typeValue != "boundary")
            {
                return false;
            }
            if (new RelationValidationTest().Visit(relation, osmSource))
            {
                return true;
            }
            return false;
        }

        public FilterSettings FilterSettings { get; } = new FilterSettings()
        {
            Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Relation, new[] { new TagFilter("boundary", "administrative") })
            }
        };

        public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevatThings, IOsmGeoBatchSource osmSource)
        {
            Utils.BatchLoad(relevatThings, osmSource, true, false);

            foreach (var relevatThing in relevatThings)
            {
                if (relevatThing is Relation relation)
                {
                    if (AnalyzeRelation(relation, osmSource))
                        yield return new IssueData()
                        {
                            IssueType = AnalyzerName,
                            OsmType = "relation",
                            OsmId = relation.Id!.Value
                        };
                }
            }
        }
    }
}
