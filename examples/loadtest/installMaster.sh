sudo apt-get update
sudo apt-get install -y software-properties-common
sudo apt-add-repository --yes --update ppa:ansible/ansible
sudo apt-get install -y ansible
sudo apt-get install -y python3-pip rsync
pip3 install --user flask gevent==1.4.0 msgpack-python pyzmq python-dateutil geoip2 pandas matplotlib==3.1.1
pushd ~
[ ! -d "ncscli" ] && git clone https://github.com/neocortix/ncscli.git
[ ! -d "locust" ] && git clone --branch 0.12.2 https://github.com/locustio/locust.git
popd
