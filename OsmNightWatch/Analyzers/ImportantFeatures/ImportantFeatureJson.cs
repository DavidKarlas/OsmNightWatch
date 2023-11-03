using OsmNightWatch.PbfParsing;
using System.Text.Json.Serialization;

namespace OsmNightWatch.Analyzers.ImportantFeatures
{
    public class ImportantFeatureJson
    {
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public OsmGeoType osm_type { get; set; }
        public long osm_id { get; set; }
        public Dictionary<string, object> tags { get; set; }
        public string wkt { get; set; }
        public string category { get; set; }
    }

}
