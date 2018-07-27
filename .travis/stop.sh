 #!/bin/bash -ex

mongo --port 27017 --eval "db.getSiblingDB('admin').shutdownServer()"
mongo --port 27018 --eval "db.getSiblingDB('admin').shutdownServer()"
mongo --port 27019 --eval "db.getSiblingDB('admin').shutdownServer()"
