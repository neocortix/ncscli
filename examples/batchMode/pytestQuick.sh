#!/usr/bin/env bash

pytest -v --rootdir . --junitxml data/pytest.xml -o 'junit_family = xunit2' \
  --deselect test_runBatchExamples.py::test_runBatchBlender \
  --deselect test_runBatchExamples.py::test_runBatchJMeter \
  --deselect  test_runBatchExamples.py::test_runBatchPuppeteerLighthouse

#   --deselect  test_runBatchExamples.py::test_runBatchPython
