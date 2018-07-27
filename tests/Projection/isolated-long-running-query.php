<?php
/**
 * This file is part of the prooph/mongodb-event-store.
 * (c) 2018 prooph software GmbH <contact@prooph.de>
 * (c) 2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\MongoDb\MongoDbEventStore;
use Prooph\EventStore\MongoDb\PersistenceStrategy\MongoDbSimpleStreamStrategy;
use Prooph\EventStore\MongoDb\Projection\MongoDbProjectionManager;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\TestDomainEvent;
use ProophTest\EventStore\MongoDb\TestUtil;

require __DIR__ . '/../../vendor/autoload.php';

$client = TestUtil::getClient();
$database = TestUtil::getDatabaseName();

//\Prooph\EventStore\MongoDb\MongoDbHelper::createEventStreamsCollection($client, TestUtil::getDatabaseName(), 'event_streams');
//\Prooph\EventStore\MongoDb\MongoDbHelper::createProjectionCollection($client, TestUtil::getDatabaseName(), 'projections');

$eventStore = new MongoDbEventStore(
    new FQCNMessageFactory(),
    $client,
    $database,
    new MongoDbSimpleStreamStrategy(new NoOpMessageConverter())
);
$events = [];

for ($i = 0; $i < 100; $i++) {
    $events[] = TestDomainEvent::with(['test' => 1], $i);
    $i++;
}

$eventStore->create(new Stream(new StreamName('user-123'), new ArrayIterator($events)));

$projectionManager = new MongoDbProjectionManager(
    $eventStore,
    $client,
    $database
);

$query = $projectionManager->createQuery(
    [
        Query::OPTION_PCNTL_DISPATCH => true,
    ]
);

\pcntl_signal(SIGQUIT, function () use ($query) {
    $query->stop();
    exit(SIGUSR1);
});

$query
    ->fromStreams('user-123')
    ->whenAny(function () {
        \usleep(500000);
    })
    ->run();
