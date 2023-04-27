using NetTopologySuite.Geometries.Prepared;
using System.Text.Json.Serialization;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
{
    public class Country
    {
        public uint RelationId { get; set; }
        public string EnglishName { get; set; }
        public string Iso2 { get; set; }
        public string Iso3 { get; set; }
        public Dictionary<string, List<long>> Admins { get; set; } = new();
        [JsonIgnore]
        public PreparedPolygon Polygon { get; set; }
        [JsonIgnore]
        public bool IsValid { get; set; }
    }

    public class StateOfTheAdmins
    {
        public StateOfTheAdmins(Dictionary<uint, HashSet<(uint CountryId, int AdminLevel)>>? adminsToCountry = null)
        {
            AdminsToCountry = adminsToCountry ?? new();
        }

        public List<Country> Countries { get; set; } = new List<Country>();

        [JsonIgnore]
        public Dictionary<uint, HashSet<(uint CountryId, int AdminLevel)>> AdminsToCountry { get; }
    }
}
