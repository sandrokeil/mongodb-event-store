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
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\TestDomainEvent;
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

$persistenceStrategy = new MongoDbSimpleStreamStrategy(new NoOpMessageConverter());

$eventStore = new MongoDbEventStore(
    new FQCNMessageFactory(),
    $client,
    $database,
    $persistenceStrategy
);
$events = [];

for ($i = 1; $i < 21; $i++) {
    $events[] = TestDomainEvent::with(['test' => $i], $i);
}

$eventStore->create(new Stream(new StreamName('user-123'), new ArrayIterator($events)));

$projectionManager = new MongoDbProjectionManager(
    $eventStore,
    $client,
    $persistenceStrategy,
    new FQCNMessageFactory(),
    $database
);

$projection = $projectionManager->createReadModelProjection(
    'test_projection',
    $readModel,
    [
        ReadModelProjector::OPTION_PCNTL_DISPATCH => true,
        ReadModelProjector::OPTION_PERSIST_BLOCK_SIZE => 2,
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
    ->whenAny(function (array $state, \Prooph\Common\Messaging\Message $event) {
        \usleep(100000);
        $state['aggregate_versions'][] = $event->metadata()['_aggregate_version'];

        return $state;
    })
    ->run(false);
