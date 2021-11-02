set -ex
cd /etc/squid/

sudo openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout bump.key -out bump.crt \
    -subj "/C=ZZ/ST=Area/L=Locality/O=Organization/OU=Unit/CN=example.com"

sudo openssl x509 -in bump.crt -outform DER -out bump.der

sudo openssl dhparam -outform PEM -out /etc/squid/bump_dhparam.pem 2048


sudo mkdir -p /var/lib/squid
sudo rm -rf /var/lib/squid/ssl_db  # in case it already existed

sudo /usr/lib/squid/security_file_certgen -c -s /var/lib/squid/ssl_db -M 20MB

# edit squid.conf to put some ssl-related directives at the end
scriptDirPath=`dirname "$0"`  # unfortunately not a full path
cat ~/$scriptDirPath/conf/sslBump.conf >> ~/$scriptDirPath/conf/squid.conf  # may require su privileges
