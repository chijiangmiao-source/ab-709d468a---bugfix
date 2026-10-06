#!/bin/sh
# Local restart hook used by verify.py when no Docker socket is available.
# Anchored patterns avoid killing the supervisor wrappers themselves.
pkill -f "^python3 -m app\.server" 2>/dev/null
pkill -f "^python3 -m app\.worker" 2>/dev/null
exit 0
