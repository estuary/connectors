#!/bin/bash

datasette serve --host 0.0.0.0 --immutable /tmp/sqlite.db &>/dev/null & disown;

exec "$@"
