namespace OsmNightWatch.Web
{
    public class Helper
    {
        public static List<(string issueType, string title, string pageUrl)> IssueTypes = new() {
            ("OpenAdminPolygon","Open admins level 0-6", "/OpenAdminPolygons" ),
            ("OpenAdminPolygon7", "Open admins level 7","/OpenAdminPolygons/7" ),
            ("OpenAdminPolygon8","Open admins level 8", "/OpenAdminPolygons/8" ),
            //("OpenAdminPolygon9","Open  admins level 9.", "/OpenAdminPolygons/9" ),
            //("OpenAdminPolygon10", "Open  admins level 10.","/OpenAdminPolygons/10" ),
            ("BrokenCoastLine", "Broken coastlines","/BrokenWaterCoastlines" )
        };

        public static string ConvertToFullOsmType(string osmType)
        {
            switch(osmType)
            {
                case "N":
                    return "node";
                case "W":
                    return "way";
                case "R":
                    return "relation";
            }
            throw new ArgumentException(osmType);
        }
    }
}
