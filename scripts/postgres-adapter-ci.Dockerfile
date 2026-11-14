# Preserve the durable-store PostgreSQL base; DB 11.0.0 probes require PostGIS
# even for nonspatial columns. Package installation failure must fail setup.
FROM postgres:17.6-bookworm@sha256:f3bd19c606e442c3d7bdfa8002e03fe260a1023351e0ea4598032022b68dd6e3
RUN apt-get update && apt-get install --no-install-recommends -y postgresql-17-postgis-3 && rm -rf /var/lib/apt/lists/*
