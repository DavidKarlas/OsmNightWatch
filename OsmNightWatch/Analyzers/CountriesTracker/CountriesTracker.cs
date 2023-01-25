using OsmNightWatch.Lib;
using OsmSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch.Analyzers.CountriesTracker {
    public class CountriesTracker : IOsmAnalyzer {
        public string AnalyzerName => throw new NotImplementedException();

        public FilterSettings FilterSettings => throw new NotImplementedException();

        public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource) {
            throw new NotImplementedException();
        }
    }
}
