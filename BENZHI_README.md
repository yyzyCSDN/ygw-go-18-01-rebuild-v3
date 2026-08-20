# Replica snapshot container

Build for amd64:

```bash
./build_benzhi_docker.sh replica-snapshot:amd64 linux/amd64
```

Build for arm64:

```bash
./build_benzhi_docker.sh replica-snapshot:arm64 linux/arm64
```

Start an interactive environment:

```bash
docker run --rm -it replica-snapshot:amd64 bash
```

Inside the container run:

```bash
go build ./...
go test ./...
go vet ./...
go run ./cmd/replica-snapshot-demo
```
