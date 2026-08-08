# syntax=docker/dockerfile:1

# ---------- build ----------
FROM golang:1.22-alpine AS build

WORKDIR /src

# GOTOOLCHAIN=local pins the build to the Go in this image.
#
# This is load-bearing, not tidiness. Go 1.21+ will silently download and switch
# to a newer toolchain whenever go.mod asks for one, which is how this project
# twice ended up believing it supported Go 1.22 while every build actually ran
# on something newer. With GOTOOLCHAIN=local the image cannot substitute a
# different compiler, so a successful build here is real evidence that the 1.22
# floor in go.mod holds.
#
# CGO_ENABLED=0 produces a static binary, which is what lets the final stage be
# an image with no libc, no shell, and nothing else to keep patched.
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0

# Dependencies are copied and downloaded first so that editing source code does
# not invalidate the cached module layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# -trimpath keeps build-machine paths out of the binary; -s -w drops the debug
# symbol table, which is a large fraction of a Go binary's size.
RUN go build -trimpath -ldflags="-s -w" -o /quorumd ./cmd/quorumd

# ---------- run ----------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /quorumd /quorumd

# 8080 is the REST API, 9090 carries Raft RPCs between nodes.
EXPOSE 8080 9090

# The image ships no shell and no curl, so the health check is this same binary
# invoked a second way rather than a tool the image would otherwise carry.
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/quorumd", "-healthcheck", "-addr", ":8080"]

USER nonroot:nonroot
ENTRYPOINT ["/quorumd"]
