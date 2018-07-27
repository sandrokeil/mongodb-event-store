# mongodb-event-store

[![Build Status](https://travis-ci.org/prooph/mongodb-event-store.svg?branch=master)](https://travis-ci.org/prooph/mongodb-event-store)
[![Coverage Status](https://coveralls.io/repos/prooph/mongodb-event-store/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/mongodb-event-store?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

MongoDB EventStore implementation for [Prooph EventStore](https://github.com/prooph/event-store)

Requirements
------------

- MongoDB >= 4.0
- MongoDB PHP Driver >= 1.5.2

Setup
-----

Please execute `\Prooph\EventStore\MongoDb\MongoDbHelper::createEventStreamsCollection` on your server.

This will setup the required event streams table.

If you want to use the projections, run additionally `\Prooph\EventStore\MongoDb\MongoDbHelper::createProjectionCollection` on your server.

Introduction
------------

[![Prooph Event Store v7](https://img.youtube.com/vi/QhpDIqYQzg0/0.jpg)](https://www.youtube.com/watch?v=QhpDIqYQzg0)

Tests
-----
If you want to run the unit tests locally you need a running MongoDB Replica cluster. You need to set these environment
variables `DB_URI`, `DB_REPLICA_SET` and `DB_NAME`.

## Run Tests With Composer

`$ vendor/bin/phpunit -c phpunit.xml.dist`

## Run Tests With Docker Compose

```bash
./start.sh
docker-compose -f docker-compose-tests.yml run --rm php php vendor/bin/phpunit -c phpunit.xml.dist
./down.sh
```

## Support

- Ask questions on Stack Overflow tagged with [#prooph](https://stackoverflow.com/questions/tagged/prooph).
- File issues at [https://github.com/prooph/event-store/issues](https://github.com/prooph/event-store/issues).
- Say hello in the [prooph gitter](https://gitter.im/prooph/improoph) chat.

## Contribute

Please feel free to fork and extend existing or add new plugins and send a pull request with your changes!
To establish a consistent code quality, please provide unit tests for all your changes and may adapt the documentation.

## License

Released under the [New BSD License](LICENSE).

