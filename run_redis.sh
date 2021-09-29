#!/usr/bin/env bash

docker stop redis || true
docker run --rm -d --name redis -p 6379:6379 -d redis