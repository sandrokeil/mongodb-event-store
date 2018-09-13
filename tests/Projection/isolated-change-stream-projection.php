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
use Prooph\EventStore\MongoDb\Projection\MongoDbEventStoreProjector;
use Prooph\EventStore\MongoDb\Projection\MongoDbProjectionManager;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\MongoDb\TestUtil;

require __DIR__ . '/../../vendor/autoload.php';

$client = TestUtil::getClient();
$database = TestUtil::getDatabaseName();

$persistenceStrategy = new MongoDbSimpleStreamStrategy(new NoOpMessageConverter());

$eventStore = new MongoDbEventStore(
    new FQCNMessageFactory(),
    $client,
    $database,
    $persistenceStrategy
);

$projectionManager = new MongoDbProjectionManager(
    $eventStore,
    $client,
    $persistenceStrategy,
    new FQCNMessageFactory(),
    $database
);
$projection = $projectionManager->createProjection(
    'test_projection',
    [
        MongoDbEventStoreProjector::OPTION_PCNTL_DISPATCH => true,
        MongoDbEventStoreProjector::OPTION_PERSIST_BLOCK_SIZE => 2,
    ]
);
\pcntl_signal(SIGQUIT, function () use ($projection) {
    $projection->stop();
    exit(SIGUSR1);
});
$projection
    ->init(function (): array {
        return ['aggregate_versions' => []];
    })
    ->fromStream('user-123')
    ->when([
        UserCreated::class => function (array $state, UserCreated $event): array {
            \usleep(100000);
            $state['aggregate_versions'][] = $event->payload()['id'];

            return $state;
        },
    ])
    ->run(false);
