# db-restore

Restore a PostgreSQL database from an AWS RDS snapshot's S3 export.

# Updating dependencies

When changing anything in dependencies, the following command has to be executed 
to update the `go.mod` and `go.sum` files:

```bash
cd src/
go mod tidy
```

Updating all dependencies to new versions:

```bash
cd src/
go get -u
```

This is especially important before building the Docker image.
TODO: Ideally, it should be automated as part of the regular build.

# Compilation

Before compilation, one must generate that Swagger documentation as described above, 
because it is embedded into the executable.

```bash
cd src
rm -f ./dbrestore && go build
```

Removal is useful because any warning or error cause the build to fail creating a new binary, 
and it is easy to miss because the old binary remains.

# Unit tests

Simple `go test` fails, so it has to be run like the following:

```bash
cd src
go test -v .
```

# Podman and Docker build

Podman is recommended because it is root-less, though Docker is supported as well.

The tag is currently specified manually and must be incremented before publishing Docker images to any online repos.

Build using this command for Podman Buildah:

```bash
buildah build -f Dockerfile -t docker-dbrestore:0.2-dev .
```

or with Docker:

```bash
docker build --tag docker-dbrestore:0.2-dev .
```
