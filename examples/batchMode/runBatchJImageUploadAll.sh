#!/usr/bin/env bash
set -e
./runBatchJImageUpload.py 2>&1 >/dev/null | ./trackProgress.py
