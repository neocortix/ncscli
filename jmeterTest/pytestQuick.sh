#!/usr/bin/env bash

pytest -v --rootdir . --junitxml data/pytest.xml -o 'junit_family = xunit2' \
  --deselect test_jmeter.py::test_moreSlowWorker \
  --deselect test_jmeter.py::test_petstoreWorker \
  --deselect test_jmeter.py::test_rampWorker \
  --deselect test_jmeter.py::test_simpleWorker \
  --ignore data

# pretty-print the xml summary, if xmllint is installed
if hash xmllint 2>/dev/null; then
    xmllint --format data/pytest.xml
fi

#   --ignore-glob apache-jmeter-5.*
