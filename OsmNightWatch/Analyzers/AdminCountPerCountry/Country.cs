using NetTopologySuite.Geometries.Prepared;
using System.Text.Json.Serialization;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
{
    public class Country
    {
        public long RelationId { get; set; }
        public string EnglishName { get; set; }
        public string Iso3 { get; set; }
        public string Iso2 { get; set; }
        public Dictionary<string, List<long>> Admins { get; set; } = new();
        [JsonIgnore]
        public PreparedPolygon Polygon { get; set; }
    }

    public class StateOfTheAdmins
    {
        public List<Country> Countries { get; set; } = new List<Country>();
    }
}
