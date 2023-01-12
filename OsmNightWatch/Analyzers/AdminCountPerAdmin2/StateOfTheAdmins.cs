namespace OsmNightWatch.Analyzers.AdminCountPerAdmin2;

/// <summary>
/// Definition of Country is OverpassTurbo ["ISO3166-1:alpha3"]["admin_level"="2"]["type"="boundary"]["boundary"="administrative"];
/// </summary>
public class Country
{
    public long RelationId { get; set; }
    public string EnglishName { get; set; }
    public string Iso3 { get; set; }
    public string Iso2 { get; set; }
    public Dictionary<string, List<long>> Admins { get; set; } = new();
}

class StateOfTheAdmins
{
    public List<Country> Countries { get; set; } = new List<Country>();
}
