#!/usr/bin/env bash

jdkVersion="$(javac -version 2>&1)"
if [ $? -eq 0 ]; then
    echo $jdkVersion already installed
    exit 1
fi

osName="$(uname -s)"
if [ "$osName" = "Linux" ]; then
    echo "Linux"
    if hash apt-get 2>/dev/null; then
        echo updating with apt-get
        sudo apt-get update
        echo installing JDK 11
        sudo apt-get install default-jdk-headless
    else
        echo "apt-get not available; please install JDK 11 manually, or use your package manager"
    fi
elif [ "$osName" = "Darwin" ]; then
    echo "macOS"
    if hash brew 2>/dev/null; then
        echo updating with homebrew
        brew update
        echo installing JDK 11
        brew install openjdk@11
    else
        echo "homebrew not installed; please install homebrew first, or install JDK 11 manually"
    fi
else
    echo "unrecognized OS"
    echo "please install JDK 11 manually, or use your package manager"
fi
