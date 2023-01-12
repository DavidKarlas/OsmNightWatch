using NetTopologySuite.Geometries;
using OsmSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public static class OsmToNtsHelper
    {
        public static Coordinate ToCoordinate(this Node node)
        {
            return new Coordinate((double)node.Longitude!, (double)node.Latitude!);
        }
    }
}
