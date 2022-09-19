using OsmSharp;
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
        public string AnalyzerName => nameof(AdminOpenPolygonAnalyzer);

        public bool AnalyzeRelation(Relation relation, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource)
        {
            if (relation.Tags.TryGetValue("admin_level", out var lvl))
            {
                if (double.Parse(lvl, CultureInfo.InvariantCulture) > 7)
                    return false;
            }
            else
            {
                return false;
            }
            if (new RelationValidationTest().Visit(relation, newOsmSource))
            {
                return true;
            }
            return false;
        }

        public IEnumerable<ElementFilter> GetFilters()
        {
            yield return new ElementFilter(OsmGeoType.Relation, new[] { new TagFilter("boundary", "administrative") });
        }
    }
}
