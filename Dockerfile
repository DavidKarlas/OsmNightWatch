# Multi-stage Dockerfile to build and run the OsmNightWatch .NET console app

# ----- Build stage -----
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
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
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS runtime
WORKDIR /app

RUN apk add --no-cache \
        sqlite-dev \
        libspatialite-dev \
        lmdb-dev

# Copy published output from build stage
COPY --from=build /app/publish .

# Run the console application
ENTRYPOINT ["dotnet", "OsmNightWatch.dll"]
