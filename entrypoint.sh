#!/bin/sh -eu

# Ensure required environment variables are set.
( : "$MONGO_HOST" )
( : "$DISCORD_TOKEN" )
( : "$TARGET_CHANNELS" )

exec "$@"
