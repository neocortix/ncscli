#!/usr/bin/env bash
set -e
./runBatchJMeter.py 2>&1 >/dev/null | ./trackProgress.py
