# Use the official MySQL 8 image as the base
FROM mysql:8

# Copy initialization SQL script into the directory that MySQL uses to initialize the DB
COPY init.sql /docker-entrypoint-initdb.d/
