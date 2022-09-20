namespace OsmNightWatch.Lib
{
    public class IssuesData
    {
        public DateTime DateTime { get; set; }

        public List<IssueData>? AllIssues { get; set;  }
    }

    public class IssueData
    {
        /// <summary>
        /// The issue type, e.g: OpenAdminPolygon, BrokenCoastLine...
        /// </summary>
        public string IssueType { get; set; }

        public string OsmType { get; set; }

        public string OsmId { get; set; }

        public string Details { get; set; }
    }
}