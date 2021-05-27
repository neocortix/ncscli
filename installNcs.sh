#!/usr/bin/env bash

echo hello

targetVersion="3.(6|7|8|9)"
errMsg="python $targetVersion is not available in this environment"

# python3 is preferred over regular python
if hash python3 2>/dev/null; then
    echo has python3
    pyx="python3"
else
    echo NO "python3"
    if hash python 2>/dev/null; then
        echo has python
        pyx="python"
    else
        echo NO python
    fi
fi


pyVersion="$($pyx -V 2>&1)"
#pyVersion="$(python3 -V 2>&1)"
#echo $pyVersion

if [[ $pyVersion =~ $targetVersion ]]
then
    echo found $pyVersion
else
    echo $errMsg
    echo installing python3
    sudo yum update
    sudo yum install python3
    if [ $? -eq 0 ]; then
        echo installed python3
    else
        echo COULD NOT INSTALL python3
        exit 1
    fi
    pyx="python3"
fi



# pip3 is preferred over regular pip
if hash pip3 2>/dev/null; then
    echo has pip3
    pipx="pip3"
else
    echo NO pip3
    if hash pip 2>/dev/null; then
        echo has pip
        pipx="pip"
    else
        echo NO pip
    fi
fi

pipVersion="$($pyx -m pip --version 2>&1)"
if [[ $pipVersion =~ $targetVersion ]]
then
    echo found $pipVersion
else
    echo pipVersion $pipVersion
    $pyx -c "import distutils.util"
    if [ $? -eq 0 ]; then
        echo has distutils
    else
        echo NO distutils
        echo apt-installing distutils
        sudo apt-get update
        sudo apt-get install python3-distutils  # would not work on redhat
    fi
    # try installing pip using get-pip
    echo installing pip
    curl -L https://bootstrap.pypa.io/get-pip.py > get-pip.py
    $pyx get-pip.py --user --upgrade --no-cache-dir

    # check whether it is installed now
    pipVersion="$($pyx -m pip --version 2>&1)"
    if [[ $pipVersion =~ $targetVersion ]]
    then
        echo found $pipVersion
    else
        echo pipVersion $pipVersion
        echo pip for $errMsg
        exit 1
    fi
fi

# install ncscli distribution from pypi
$pyx -m pip install --user --upgrade --no-cache-dir ncscli

# install matplotlib for use by examples
$pyx -m pip install --user matplotlib

# install enlighten for use by examples
$pyx -m pip install --user enlighten


sitePackagesDir=$($pyx -m site --user-site)
examplesSrcDir=$sitePackagesDir/ncsexamples/
if [ -d "ncsexamples" ]; then
  echo "leaving your ncsexamples dir unmodified"
  echo \(upated examples may be found in $examplesSrcDir\)
else
    # copy examples dir to a convenient editable location
    echo copying $examplesSrcDir to ncsexamples
    cp -p -r $examplesSrcDir ncsexamples
fi

echo finished

