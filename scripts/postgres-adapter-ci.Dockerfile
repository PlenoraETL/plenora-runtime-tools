# DB 11.0.0 probes require PostGIS even for nonspatial columns. Server and
# extension come from one published image fixed by digest, the same reviewed
# pin as the database-tools PostgreSQL matrix (scripts/check_postgres_matrix.py):
# an apt install on a pinned base drifted to a PostGIS built for another minor.
FROM postgis/postgis:17-3.5@sha256:624f5195b91d424dbebf018890148cc0e5a3e80db5467da8b53cc2ed2ce49216
