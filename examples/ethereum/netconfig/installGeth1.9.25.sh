#!/usr/bin/env bash
set -ex

curl -L  https://gethstore.blob.core.windows.net/builds/geth-alltools-linux-arm64-1.9.25-e7872729.tar.gz > geth.tar.gz
tar -xzf geth.tar.gz
mv geth-alltools-linux-arm64-1.9.25-e7872729 geth_1.9.25
ln -s $HOME/geth_1.9.25/geth /usr/bin/geth

python3 -c 'import uuid; print( str( uuid.uuid4() ) )' > pw.txt
