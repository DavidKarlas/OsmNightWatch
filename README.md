# OsmNightWatch

Source reposity for https://nightwatch.openstreetmap.si/.

# Problem & Solution

## Problem

Anyone making high quality global map from OpenStreetMap(OSM) on regular candance knows how fast things like coastlines and administrative boundaries get broken. There are multiple possible aproaches to address these problems e.g. validation of indiviual relations and keeping old version if relation is broken or connecting closests coastline nodes to make continent whole. But most of this aproaches are prone to weird unpredicted issues like missaligned polygons or weird looking waters.

## Solution
I came up with idea, what if we monitored OSM in real-time and fix all issues on OSM and pull data to good version of OSM when starting pipeline. So during Microsoft 2022 Hackaton me and Milica started coding and in first version we just monitored natural=coastline. This allowed us to produce good coastlines every week, we later expanded to also include `boundary=administrative` relations. Today we also have concept of "AdminState" which tracks exact list of specific `admin_level=#` per country. We mostly monitor countries and their first two subdivisions. We also plan to monitor lakes/rivers as well as compare sizes of administrative boundaries and detect overlaps on same level...

# Technical implementation

## Website

Website is very simple presentation of json file kept on storage. It could really be static html+javascript page.

## Console app processing data

This is brains of this operation, it downloads OSM data and keeps processing it every minute and publishes results to storage for website to consume and present.

### Download of data

#### Initial download of data

When console app is started it checks if planet.pbf is paresent if not it downloads using [MonoTorrent](https://github.com/alanmcgovern/monotorrent) library. Once done it creates planet.pbf.index which is custom <1MB file that contains lookup table over .pbf blobs since nodes/ways/relations in .pbf are sorted which allows us to do binary lookup on blobs which are ~4MB. So to look up specific node we just need to load 4MB blob and decode that, not whole .pbf.

#### Minutely .osc files pulling

Once planet.pbf is processed we start downloading .osc files to update to latest version of OSM database. These are published every minute. All changes are stored in LMDB database which is fast in-process key-value lookup library. Using [OsmDatabaseWithReplicationData.cs](https://github.com/DavidKarlas/OsmNightWatch/blob/main/OsmNightWatch/OsmDatabaseWithReplicationData.cs) we can access data transparently as if all is latest it first tries to load data from most recently downloaded minutly .osc file, if there is no hit it looks into LMDB storage and if not hit there it goes into planet.pbf. But since planet.pbf is index and blobs are parsed in parallel on multiple threads these lookups are quick.

We also utilize [RelationChangesTracker.cs](https://github.com/DavidKarlas/OsmNightWatch/blob/main/OsmNightWatch/Analyzers/AdminCountPerCountry/RelationChangesTracker.cs) which keeps track of node->way=>relations mapping which allows us to process ways/relations lazily if their member nodes/ways change.

### Analyzers
We have few different analyzers we plan to grow them over time, each analyzer monitors and reports different things.

#### BrokenCoastline
This analyzer keeps track of all `natural=coastline` in the world and is making sure that all have first and last node in another `natural=coastline`. Beside that it also checks if any coastline spatially intersects with another and if islands are counter-clockwise. This analyzer is utilizing SQLite which has Spatial index to fetch coastlines in bounding box of newly changed coastline.

#### AdminCountPerCountry
This one started just checking if `admin_level<7` relations form valid polygon, but since advanced to look at list of admins per country as well as admin centres. Plan is to include monitoring area size, overlaps between admins and so on. This analyzer also utilizes SQLite with Spatial Index to pull same admin levels intersecting with country to check new state which is compared to [ExpectedStateOfAdmins](https://github.com/DavidKarlas/OsmNightWatch/tree/main/ExpectedStateOfAdmins).

#### ImportantFeatures
Takes important-features.json from [Daylight](https://daylightmap.org/) projects and check if all tags are same.

### Resources needed

It uses 16 GB of RAM, once initial .pbf is processed which takes around 1 hour usage goes down to minimal, it processes minutly change in few seconds.
Storage usage:
 * 82GB+ for planet.pbf
 * 3GB coastlines SQLite
 * 10GB admins SQLite
 * 10GB per month of changesets(can be reset with newer planet.pbf)

## Wanted Features / TODO

 * [Per country RSS feed](https://github.com/DavidKarlas/OsmNightWatch/issues/10) to allow local communites jump on broken admins in their countries
 * New Analyzer for water bodies: [Issue](https://github.com/DavidKarlas/OsmNightWatch/issues/17)/[Prototype branch](https://github.com/DavidKarlas/OsmNightWatch/tree/waterBodiesPrototype)
 * There is also one big project that I hope to get to one day, minutely monitoring of road network, this will be a bit tougher nut to track but I have some ideas :)
 * And all other features requested in [Issues](https://github.com/DavidKarlas/OsmNightWatch/issues)