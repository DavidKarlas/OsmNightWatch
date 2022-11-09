namespace OsmNightWatch.Lib
{
    public class IssuesData
    {
        public DateTime DateTime { get; set; }

        public List<IssueData> AllIssues { get; } = new();

        public void AddTimestamps(IssuesData? oldIssuesData)
        {
            if (oldIssuesData == null)
            {
                foreach (var issue in AllIssues)
                {
                    issue.FirstTimeSeen = DateTime;
                }
                return;
            }
            var hashset = new HashSet<IssueData>(oldIssuesData.AllIssues);
            foreach (var issue in AllIssues)
            {
                if (hashset.TryGetValue(issue, out var oldIssue))
                {
                    issue.FirstTimeSeen = oldIssue.FirstTimeSeen;
                }
                issue.FirstTimeSeen = DateTime;
            }
        }
    }

    public class IssueData
    {
        /// <summary>
        /// The issue type, e.g: OpenAdminPolygon, BrokenCoastLine...
        /// </summary>
        public string IssueType { get; set; }

        public string OsmType { get; set; }

        public long OsmId { get; set; }

        public string Details { get; set; }

        public DateTime FirstTimeSeen { get; set; }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                IssueType.GetHashCode(),
                OsmType.GetHashCode(),
                OsmId.GetHashCode(),
                Details.GetHashCode());
        }

        public override bool Equals(object? obj)
        {
            if (obj is not IssueData other) return false;
            return other.IssueType == IssueType &&
                other.OsmType == OsmType &&
                other.OsmId == OsmId &&
                other.Details == Details;
        }
    }
}