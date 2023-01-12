using System.Collections.Generic;

namespace OsmNightWatch.Lib
{
    public class IssuesData
    {
        public static HashSet<string> LastKnownGoodIssueTypes { get; } = new HashSet<string>()
        {
            "OpenAdminPolygon",
            //"OpenAdminPolygon7",
            //"OpenAdminPolygon8",
            //"OpenAdminPolygon9",
            //"OpenAdminPolygon10",
            "BrokenCoastLine"
        };
        
        public DateTime DateTime { get; set; }
        public int MinutelySequenceNumber { get; set; }

        public DateTime LastKnownGoodDateTime { get; set; }
        public int LastKnownGoodMinutelySequenceNumber { get; set; }

        public List<IssueData> AllIssues { get; init; } = new List<IssueData>();

        public void SetTimestampsAndLastKnownGood(IssuesData? oldIssuesData)
        {
            var hashSet = oldIssuesData == null ? new HashSet<IssueData>() : new HashSet<IssueData>(oldIssuesData.AllIssues);
            bool canBeLastKnownGood = true;
            foreach (var issue in AllIssues)
            {
                // Check if issue is in last known good category...
                if (LastKnownGoodIssueTypes.Contains(issue.IssueType))
                {
                    canBeLastKnownGood = false;
                }
                
                if (hashSet.TryGetValue(issue, out var oldIssue))
                {
                    issue.FirstTimeSeen = oldIssue.FirstTimeSeen;
                }
                else
                {
                    issue.FirstTimeSeen = DateTime;
                }
            }
            if (canBeLastKnownGood)
            {
                LastKnownGoodDateTime = DateTime;
                LastKnownGoodMinutelySequenceNumber = MinutelySequenceNumber;
            }
            else
            {
                LastKnownGoodDateTime = oldIssuesData?.LastKnownGoodDateTime ?? default;
                LastKnownGoodMinutelySequenceNumber = oldIssuesData?.LastKnownGoodMinutelySequenceNumber ?? -1;
            }
        }
    }

    public class IssueData
    {
        public string FriendlyName { get; set; }

        /// <summary>
        /// The issue type, e.g: OpenAdminPolygon, BrokenCoastLine...
        /// </summary>
        public string IssueType { get; set; }

        public string OsmType { get; set; }

        public long OsmId { get; set; }

        public string Details { get; set; }

        public DateTime? FirstTimeSeen { get; set; }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                IssueType?.GetHashCode(),
                OsmType?.GetHashCode(),
                OsmId.GetHashCode(),
                Details?.GetHashCode());
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