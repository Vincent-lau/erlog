#!/usr/bin/env sh

echo "======generating ans for $1==========="
souffle -D. $1
