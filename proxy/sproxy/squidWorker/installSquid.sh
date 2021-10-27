version=${1:-squidVersionNotSet}

apt-get -qq update > /dev/null
apt-get -qq install -y squid  > /dev/null

squid --version
