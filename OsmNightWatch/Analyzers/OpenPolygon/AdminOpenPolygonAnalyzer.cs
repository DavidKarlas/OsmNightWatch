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

        private bool AnalyzeRelation(Relation relation, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource)
        {
            if (relation.Tags.TryGetValue("admin_level", out var lvl))
            {
                if (double.Parse(lvl, CultureInfo.InvariantCulture) > 6)
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
            if (new RelationValidationTest().Visit(relation, newOsmSource))
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

        public IEnumerable<IssueData> Initialize(IEnumerable<OsmGeo> relevatThings, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource)
        {
            Utils.BatchLoad(relevatThings, (IOsmGeoBatchSource)newOsmSource, true, false);

            foreach (var relevatThing in relevatThings)
            {
                if (relevatThing is Relation relation)
                {
                    if (AnalyzeRelation(relation, oldOsmSource, newOsmSource))
                        yield return new IssueData()
                        {
                            IssueType = AnalyzerName,
                            OsmType = "relation",
                            OsmId = relation.Id!.Value
                        };
                }
            }
        }

        public IEnumerable<IssueData> AnalyzeChanges(OsmChange changeset, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource)
        {
            foreach (var relation in changeset.Delete.Concat(changeset.Create).Concat(changeset.Modify).OfType<Relation>())
            {
                if (AnalyzeRelation(relation, oldOsmSource, newOsmSource))
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
