set -ex
cd /etc/squid/

sudo openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout bump.key -out bump.crt \
    -subj "/C=ZZ/ST=Area/L=Locality/O=Organization/OU=Unit/CN=example.com"

sudo openssl x509 -in bump.crt -outform DER -out bump.der

sudo openssl dhparam -outform PEM -out /etc/squid/bump_dhparam.pem 2048

#sudo chown proxy:proxy /etc/squid/bump*
#sudo chmod 400 /etc/squid/bump*

# squid -v to determine the squid version

# assume that squid is not currently running (or do something to stop it)
#sudo systemctl stop squid

#sudo mkdir -p /var/spool/squid
sudo mkdir -p /var/lib/squid
sudo rm -rf /var/lib/squid/ssl_db  # in case it already existed

#sudo /usr/lib/squid/security_file_certgen -c -s /var/spool/squid/ssl_db -M 20MB
sudo /usr/lib/squid/security_file_certgen -c -s /var/lib/squid/ssl_db -M 20MB
#sudo chown -R proxy:proxy /var/lib/squid

# create or edit /etc/squid/conf.d/access.conf to prepend  acl and http_access lines

# edit squid.conf to put some ssl-related directives at the end
scriptDirPath=`dirname "$0"`  # unfortunately not a full path
cat ~/$scriptDirPath/conf/sslBump.conf >> ~/$scriptDirPath/conf/squid.conf  # may require su privileges

# for reference, this is typically the contents of sslBump.conf
#sslcrtd_program /usr/lib/squid/security_file_certgen -s /var/lib/squid/ssl_db -M 20MB
#sslproxy_cert_error allow all
#ssl_bump stare all

# edit squid.conf to change the http_port directive
#http_port 3128 tcpkeepalive=60,30,3 ssl-bump generate-host-certificates=on dynamic_cert_mem_cache_size=20MB tls-cert=/etc/squid/bump.crt tls-key=/etc/squid/bump.key cipher=HIGH:MEDIUM:!LOW:!RC4:!SEED:!IDEA:!3DES:!MD5:!EXP:!PSK:!DSS options=NO_TLSv1,NO_SSLv3,SINGLE_DH_USE,SINGLE_ECDH_USE tls-dh=prime256v1:/etc/squid/bump_dhparam.pem

