#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="hydro-claude"
CONTAINER_NAME="hydro-claude-session"

# API key: passed via env var, or Claude reads it from ~/.claude/api-settings.json apiKeyHelper
API_KEY_ARGS=()
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    API_KEY_ARGS+=(-e "ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY")
fi

# Build the image if needed
if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
    echo "Building Docker image '$IMAGE_NAME'..."
    docker build -f Dockerfile.claude -t "$IMAGE_NAME" .
fi

# Map host user into the container so $HOME paths match
HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
HOST_HOME="$HOME"

# Use -it only when running in a terminal
TTY_ARGS=()
if [ -t 0 ]; then
    TTY_ARGS+=(-it)
fi

# Pass all script arguments through to claude
# Examples:
#   ./run-claude-docker.sh -p "explain this codebase"
#   ./run-claude-docker.sh -p "fix the build" --output-format json
#   ./run-claude-docker.sh                        # interactive mode
# Mount host shared libraries that host-built binaries (e.g. bd) need.
# The container may ship a different ICU version than the host.
HOST_LIB_MOUNTS=()
for lib in /lib/x86_64-linux-gnu/libicu{i18n,uc,data}.so.*; do
    [ -f "$lib" ] && HOST_LIB_MOUNTS+=(-v "$lib:$lib:ro")
done

exec docker run --rm "${TTY_ARGS[@]}" \
    --name "$CONTAINER_NAME" \
    --user "$HOST_UID:$HOST_GID" \
    -e HOME="$HOST_HOME" \
    -e PATH="$HOST_HOME/.local/bin:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    "${API_KEY_ARGS[@]}" \
    "${HOST_LIB_MOUNTS[@]}" \
    -v "$HOST_HOME:$HOST_HOME" \
    -v "$(pwd):/workspace" \
    "$IMAGE_NAME" \
    "$@"
