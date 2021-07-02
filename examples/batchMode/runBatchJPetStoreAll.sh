#!/usr/bin/env bash
set -e
./runBatchJPetStore.py 2>&1 >/dev/null | ./trackProgress.py
