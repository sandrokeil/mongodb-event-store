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
use Prooph\EventStore\MongoDb\Projection\MongoDbEventStoreReadModelProjector;
use Prooph\EventStore\MongoDb\Projection\MongoDbProjectionManager;
use Prooph\EventStore\Projection\ReadModel;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\MongoDb\TestUtil;

require __DIR__ . '/../../vendor/autoload.php';

$readModel = new class() implements ReadModel {
    public function init(): void
    {
    }

    public function isInitialized(): bool
    {
        return true;
    }

    public function reset(): void
    {
    }

    public function delete(): void
    {
    }

    public function stack(string $operation, ...$args): void
    {
    }

    public function persist(): void
    {
    }
};

$client = TestUtil::getClient();
$database = TestUtil::getDatabaseName();

$eventStore = new MongoDbEventStore(
    new FQCNMessageFactory(),
    $client,
    $database,
    new MongoDbSimpleStreamStrategy(new NoOpMessageConverter())
);

$projectionManager = new MongoDbProjectionManager(
    $eventStore,
    $client,
    $database
);
$projection = $projectionManager->createReadModelProjection(
    'test_projection',
    $readModel,
    [
        MongoDbEventStoreReadModelProjector::OPTION_PCNTL_DISPATCH => true,
        MongoDbEventStoreReadModelProjector::OPTION_LOCK_TIMEOUT_MS => 3000,
        MongoDbEventStoreReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD => 2000,
    ]
);
\pcntl_signal(SIGQUIT, function () use ($projection) {
    $projection->stop();
    exit(SIGUSR1);
});
$projection
    ->fromStream('user-123')
    ->when([
        UserCreated::class => function (array $state, UserCreated $event): array {
            return $state;
        },
    ])
    ->run();
