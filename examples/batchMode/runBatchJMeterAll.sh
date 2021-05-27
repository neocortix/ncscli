#!/usr/bin/env bash
./runBatchJMeter.py 2>&1 >/dev/null | ./trackProgress.py
