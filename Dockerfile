# syntax=docker/dockerfile:1

# freeathome2mqtt container image (docs/00 §5; docs/11 WP12).
#
# Multi-stage: `builder` resolves the exact pinned interpreter/dependencies with `uv` into a
# self-contained virtualenv; `runtime` copies only that venv plus the installed package into a
# fresh `python:3.14.7-slim` image, so the final image carries no build toolchain, no `uv`, no
# apt cache. Built via `docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7`
# (docs/00 §5) -- the pinned 3.14.7 base matches `.python-version`/`requires-python` exactly, per
# that section's own "bump all three together, deliberately" rule.

FROM python:3.14.7-slim AS builder

# uv (docs/00 §5's tooling choice), pinned like every other dependency in this project -- via pip
# from PyPI rather than copying the astral-sh/uv image's binary, so the version is verifiable
# against the same index `uv.lock` itself resolves against, with no second registry to trust.
RUN pip install --no-cache-dir --root-user-action=ignore uv==0.12.9

WORKDIR /build
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

# Dependencies first, source second: the dependency layer only invalidates when pyproject.toml/
# uv.lock actually change, not on every source edit. --no-editable: the runtime stage below copies
# only .venv, not /build/src, so the venv must be fully self-contained rather than a .pth pointing
# back at a source tree that will no longer exist.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev --no-editable

COPY src/ src/
COPY README.md LICENSE ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.14.7-slim AS runtime

# A dedicated, unprivileged user at a fixed, documented uid/gid (one of WP12's own deliverables,
# docs/11) -- the bridge only ever talks outbound HTTP/WS/MQTT and writes its own data directory,
# so it needs no elevated privilege at all. The fixed 10001:10001 (rather than a floating --system
# id) is so a host bind-mount for /data can be `chown -R 10001:10001` once, predictably --
# docker-compose.example.yml documents this.
RUN groupadd --gid 10001 freeathome2mqtt \
    && useradd --uid 10001 --gid freeathome2mqtt --home-dir /data --create-home freeathome2mqtt

WORKDIR /app
COPY --from=builder --chown=freeathome2mqtt:freeathome2mqtt /build/.venv /app/.venv

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

VOLUME ["/data"]
USER freeathome2mqtt

# Run this image with `docker run --init` (or docker-compose.example.yml's `init: true`) so PID 1
# reaps zombies and forwards SIGTERM/SIGINT correctly (docs/08 §10's graceful shutdown depends on
# actually receiving the signal) -- deliberately not baking `tini` into the image itself: Docker's
# own `--init` does the identical job for zero extra image weight.
#
# The healthcheck only confirms the mounted config.yaml still parses and validates -- the same
# check CI's own container smoke test runs (docs/10 §9) -- not live SysAP/MQTT connectivity, which
# `bridge/state` and the container logs are the right tool for instead. A documented
# simplification, not a silent gap: the process itself already exits non-zero on
# TaskDiedTooManyTimesError (docs/02 §3.1), so the container's own restart policy is what recovers
# from a genuinely dead supervisor loop.
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD ["freeathome2mqtt", "--check-config"]

ENTRYPOINT ["freeathome2mqtt"]
