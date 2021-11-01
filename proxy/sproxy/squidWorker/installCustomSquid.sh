set -ex
version=${1:-squidVersionNotSet}

apt-get -qq update > /dev/null
#apt-get -qq install -y squid  > /dev/null

scriptDirPath=`dirname "$0"`

# install squid packages
SQUID_PKG=4.13-10
apt-get install -y libecap3 libltdl7 libnetfilter-conntrack3 libxml2 logrotate libdbi-perl
apt-get install -y squid-langpack
pushd $scriptDirPath/pkg
    dpkg --install squid-common_${SQUID_PKG}_all.deb
    dpkg --install squid-openssl_${SQUID_PKG}_arm64.deb
    dpkg --install squidclient_${SQUID_PKG}_arm64.deb
popd

#tar -zxf $scriptDirPath/bin/squid.tar.gz -C /usr/sbin
#cp -p $scriptDirPath/bin/squid /usr/sbin

# configure squid for sslbump (https re-encryption)
pushd .
    $scriptDirPath/configureSslBump.sh
popd


squid --version
