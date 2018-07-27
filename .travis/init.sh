 #!/bin/bash -ex

mkdir -p $(pwd)/db1
mkdir -p $(pwd)/db2
mkdir -p $(pwd)/db3

mongod --port 27017 --bind_ip 0.0.0.0 --fork --logpath $(pwd)/log1.log --dbpath $(pwd)/db1 --replSet repl0
mongod --port 27018 --bind_ip 0.0.0.0 --fork --logpath $(pwd)/log2.log --dbpath $(pwd)/db2 --replSet repl0
mongod --port 27019 --bind_ip 0.0.0.0 --fork --logpath $(pwd)/log3.log --dbpath $(pwd)/db3 --replSet repl0

sleep 3

mongo --eval "rs.initiate({'_id':'repl0',members:[{'_id':0,'host':'127.0.0.1:27017'},{'_id':1,'host':'127.0.0.1:27018'},{'_id':2,'host':'127.0.0.1:27019'}]})"
