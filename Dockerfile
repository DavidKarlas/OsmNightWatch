# Multi-stage Dockerfile to build and run the OsmNightWatch .NET console app

# ----- Build stage -----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# # Copy csproj files first to leverage Docker layer caching for restore
# COPY ["OsmNightWatch/OsmNightWatch.csproj", "OsmNightWatch/"]
# COPY ["OsmNightWatch.Lib/OsmNightWatch.Lib.csproj", "OsmNightWatch.Lib/"]
# COPY ["PbfParser/PbfParser.csproj", "PbfParser/"]
# COPY ["OsmNightWatch/OsmSharp.Replication/OsmSharp.Replication.csproj", "OsmNightWatch/OsmSharp.Replication/"]

# # Restore dependencies
# RUN dotnet restore "OsmNightWatch/OsmNightWatch.csproj"

# Copy the entire source and build/publish
COPY . .
RUN dotnet publish "OsmNightWatch/OsmNightWatch.csproj" -c Release -o /app/publish

# ----- Runtime stage -----
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app

# Optional native dependencies commonly required by packages used in this repo
# - libsqlite3-mod-spatialite: SpatiaLite extension for SQLite (Linux)
# - liblmdb0: LightningDB/LMDB native library
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libsqlite3-mod-spatialite \
        liblmdb0 && \
    rm -rf /var/lib/apt/lists/*

# Copy published output from build stage
COPY --from=build /app/publish .

# Run the console application
ENTRYPOINT ["dotnet", "OsmNightWatch.dll"]
