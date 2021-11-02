set -ex
version=${1:-squidVersionNotSet}

apt-get -qq update > /dev/null

scriptDirPath=`dirname "$0"`

# install squid packages
SQUID_PKG=4.13-10
apt-get -qq install -y libecap3 libltdl7 libnetfilter-conntrack3 libxml2 logrotate libdbi-perl > /dev/null
apt-get -qq install -y squid-langpack  > /dev/null
pushd $scriptDirPath/pkg
    dpkg --install squid-common_${SQUID_PKG}_all.deb > /dev/null
    dpkg --install squid-openssl_${SQUID_PKG}_arm64.deb  > /dev/null
    dpkg --install squidclient_${SQUID_PKG}_arm64.deb  > /dev/null
popd

# configure squid for sslbump (https re-encryption)
pushd .
    $scriptDirPath/configureSslBump.sh
popd

squid --version
