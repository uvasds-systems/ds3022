# Building and Pushing the Producer Image to ghcr.io

To containerize `wikipedia-producer.py` in your own repo, you need a `Dockerfile`, a `requirements.txt`, and a GitHub Actions workflow that builds the image and pushes it to the GitHub Container Registry (ghcr.io).

**You do not need to create a token or add a repository secret.** Every workflow run gets a built-in `GITHUB_TOKEN`, and that token can push to ghcr.io once the workflow grants it permission.

## The workflow

This workflow does three things:
- It runs when you push a version tag such as `1.1`, `1.2` or `1.35`. Ordinary commits don't trigger a build.
- It tags the image with that version and with `latest`.
- It builds for both `linux/amd64` (Intel/AMD machines, most cloud servers) and `linux/arm64` (Apple Silicon Macs, Raspberry Pi, AWS Graviton).

Save this as `.github/workflows/docker.yml` in your separate repository:

```yaml
name: Build and push image

on:
  push:
    tags:
      - '[0-9]+.[0-9]+'   # matches 1.1, 1.2, 1.35, ...

permissions:
  contents: read     # check out the code
  packages: write    # push to ghcr.io (required)

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # QEMU emulates arm64 on the amd64 runner; Buildx builds multiple platforms at once
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3

      # Log in to ghcr.io with the built-in token (no secret to create)
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      # Produce a lowercase image name and two tags: the git tag (e.g. 1.2) and latest
      - id: meta
        uses: docker/metadata-action@v5
        with:
          images: ghcr.io/${{ github.repository }}
          tags: |
            type=ref,event=tag
            type=raw,value=latest

      # Build both architectures and push them as one multi-arch image
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

To release a version, commit your changes, then create and push a tag:

```bash
git tag 1.2
git push origin 1.2
```

A plain `git push` does not send tags. The tag has to be pushed explicitly, as shown.

If Claude Code writes the workflow for you, check that it has the `packages: write` permission. Without it, the push fails with a `403 Forbidden` error.

## Common problems

- **Pushing a tag doesn't start a build.** Check that the tag was actually pushed: `git push` alone doesn't send tags. Also check that the tag matches the pattern. `v1.2` and `1.2.0` don't match `[0-9]+.[0-9]+`. Either use the `1.2` form or change the pattern in `on.push.tags`.
- **The `latest` tag isn't added.** `docker/metadata-action` adds `latest` automatically only in some cases. The example adds it explicitly with `type=raw,value=latest`, so every tagged build also updates `latest`. Don't use `type=semver` for tags like `1.35`, because semver requires three parts (`1.35.0`).
- **Only one architecture was built.** `platforms: linux/amd64,linux/arm64` needs both the `setup-qemu-action` and `setup-buildx-action` steps. Without them, the build fails or produces an image for amd64 only. You can check what was pushed with `docker buildx imagetools inspect ghcr.io/<user>/<repo>:latest`, which lists one entry per platform.
- **The base image must support both architectures.** Official images such as `python:3.13-slim` do. A base image that exists only for amd64 makes the arm64 build fail.
- **Multi-arch builds are slow.** The arm64 half runs under emulation, so a build can take several times longer than amd64 alone. Pip packages without prebuilt arm64 wheels compile from source, which is slower still. For this producer, `quixstreams` and `requests-sse` install from prebuilt wheels on both architectures, so this shouldn't come up.
- **`exec format error` when the container starts.** The image was built for a different architecture than the machine running it. This usually means a single-arch build: rebuild with both platforms.
- **Uppercase in the image name.** ghcr.io requires lowercase image names. Writing `ghcr.io/${{ github.repository }}` straight into a tag fails if your GitHub username has capital letters. The `metadata-action` step above lowercases the name for you.
- **The package is private.** The first push creates a private package. Pulling it then requires logging in with `docker login ghcr.io`. To allow anyone to pull it, open the package page (your profile → Packages), go to Package settings, and choose Change visibility.
- **Pull requests from forks can't push.** Workflows triggered by a PR from a fork get a read-only token. This doesn't affect tags you push to your own repo.
- **The package already exists.** If you created a package with the same name earlier (for example, by pushing with a personal access token), this repo's workflow may not have write access to it. In the package settings, add your repo under "Manage Actions access" with the Write role, or delete the package and let the workflow create it again.

## When you would need a personal access token

Only when pushing to a package owned by a different repo or account. That doesn't apply to this assignment.

## Pulling and running the image

With the brokers running under `06_kafka/docker-compose.yml`:

```bash
docker pull ghcr.io/<your-username>/<your-repo>:latest
docker run --rm --network stream_network \
  -e KAFKA_BROKER=redpanda-0:9092,redpanda-1:9092,redpanda-2:9092 \
  ghcr.io/<your-username>/<your-repo>:latest
```

The producer reads `KAFKA_BROKER` from the environment, and its default (`127.0.0.1:19092,...`) works only when the script runs directly on your machine. Inside a container, `127.0.0.1` refers to the container itself. `host.docker.internal:19092` doesn't work either: that port's listener advertises its address as `127.0.0.1`, so the client is sent back to the container's own address. Instead, attach the container to the brokers' network (`stream_network`) and use their internal addresses, as shown above.
