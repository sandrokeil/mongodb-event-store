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

namespace Prooph\EventStore\MongoDb\Projection;

use MongoDB\Client;
use MongoDB\Driver\Exception\Exception as MongoDbException;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Exception\ProjectionNotFound;
use Prooph\EventStore\MongoDb\Exception;
use Prooph\EventStore\MongoDb\MongoDbHelper;
use Prooph\EventStore\MongoDb\MongoEventStore;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;

final class MongoDbProjectionManager implements ProjectionManager
{
    use MongoDbHelper;

    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $database;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var PersistenceStrategy
     */
    private $persistenceStrategy;

    public function __construct(
        EventStore $eventStore,
        Client $client,
        PersistenceStrategy $persistenceStrategy,
        MessageFactory $messageFactory,
        string $database,
        string $eventStreamsTable = 'event_streams',
        string $projectionsTable = 'projections'
    ) {
        $this->eventStore = $eventStore;
        $this->client = $client;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->messageFactory = $messageFactory;
        $this->database = $database;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof MongoEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function createQuery(array $options = []): Query
    {
        return new MongoDbEventStoreQuery(
            $this->eventStore,
            $this->client,
            $this->database,
            $this->eventStreamsTable,
            $options[Query::OPTION_PCNTL_DISPATCH] ?? Query::DEFAULT_PCNTL_DISPATCH
        );
    }

    public function createProjection(
        string $name,
        array $options = []
    ): Projector {
        return new MongoDbEventStoreProjector(
            $this->eventStore,
            $this->client,
            $this->persistenceStrategy,
            $this->messageFactory,
            $this->database,
            $name,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[MongoDbEventStoreProjector::OPTION_LOCK_TIMEOUT_MS] ?? MongoDbEventStoreProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[MongoDbEventStoreProjector::OPTION_CACHE_SIZE] ?? MongoDbEventStoreProjector::DEFAULT_CACHE_SIZE,
            $options[MongoDbEventStoreProjector::OPTION_PERSIST_BLOCK_SIZE] ?? MongoDbEventStoreProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[MongoDbEventStoreProjector::OPTION_SLEEP] ?? MongoDbEventStoreProjector::DEFAULT_SLEEP,
            $options[MongoDbEventStoreProjector::OPTION_PCNTL_DISPATCH] ?? MongoDbEventStoreProjector::DEFAULT_PCNTL_DISPATCH,
            $options[MongoDbEventStoreProjector::OPTION_UPDATE_LOCK_THRESHOLD] ?? MongoDbEventStoreProjector::DEFAULT_UPDATE_LOCK_THRESHOLD
        );
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        array $options = []
    ): ReadModelProjector {
        return new MongoDbEventStoreReadModelProjector(
            $this->eventStore,
            $this->client,
            $this->persistenceStrategy,
            $this->messageFactory,
            $this->database,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[MongoDbEventStoreReadModelProjector::OPTION_LOCK_TIMEOUT_MS] ?? MongoDbEventStoreReadModelProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[MongoDbEventStoreReadModelProjector::OPTION_PERSIST_BLOCK_SIZE] ?? MongoDbEventStoreReadModelProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[MongoDbEventStoreReadModelProjector::OPTION_SLEEP] ?? MongoDbEventStoreReadModelProjector::DEFAULT_SLEEP,
            $options[MongoDbEventStoreReadModelProjector::OPTION_PCNTL_DISPATCH] ?? MongoDbEventStoreReadModelProjector::DEFAULT_PCNTL_DISPATCH,
            $options[MongoDbEventStoreReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD] ?? MongoDbEventStoreReadModelProjector::DEFAULT_UPDATE_LOCK_THRESHOLD
        );
    }

    public function deleteProjection(string $name, bool $deleteEmittedEvents): void
    {
        if ($deleteEmittedEvents) {
            $status = ProjectionStatus::DELETING_INCL_EMITTED_EVENTS()->getValue();
        } else {
            $status = ProjectionStatus::DELETING()->getValue();
        }

        try {
            $result = $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $name,
                ],
                [
                    '$set' => [
                        'status' => $status,
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getMatchedCount()) {
            throw ProjectionNotFound::withName($name);
        }
    }

    public function resetProjection(string $name): void
    {
        try {
            $result = $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $name,
                ],
                [
                    '$set' => [
                        'status' => ProjectionStatus::RESETTING()->getValue(),
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getMatchedCount()) {
            throw ProjectionNotFound::withName($name);
        }
    }

    public function stopProjection(string $name): void
    {
        try {
            $result = $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $name,
                ],
                [
                    '$set' => [
                        'status' => ProjectionStatus::STOPPING()->getValue(),
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getMatchedCount()) {
            throw ProjectionNotFound::withName($name);
        }
    }

    public function fetchProjectionNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        if (1 > $limit) {
            throw new OutOfRangeException(
                'Invalid limit "' . $limit . '" given. Must be greater than 0.'
            );
        }

        if (0 > $offset) {
            throw new OutOfRangeException(
                'Invalid offset "' . $offset . '" given. Must be greater or equal than 0.'
            );
        }
        $where = [];

        if (null !== $filter) {
            $where['name'] = $filter;
        }

        try {
            if (! $this->checkCollectionExists($this->projectionsTable)) {
                throw Exception\CollectionNotSetupException::with($this->projectionsTable);
            }
            $cursor = $this->collection($this->projectionsTable)->find(
                $where,
                [
                    'projection' => ['name' => 1],
                    'sort' => ['name' => 1],
                    'limit' => $limit,
                    'skip' => $offset,
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        $projectionNames = [];

        foreach ($cursor as $projection) {
            $projectionNames[] = $projection['name'];
        }

        return $projectionNames;
    }

    public function fetchProjectionNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        if (1 > $limit) {
            throw new OutOfRangeException(
                'Invalid limit "' . $limit . '" given. Must be greater than 0.'
            );
        }

        if (0 > $offset) {
            throw new OutOfRangeException(
                'Invalid offset "' . $offset . '" given. Must be greater or equal than 0.'
            );
        }

        if (empty($filter) || false === @\preg_match("/$filter/", '')) {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        }

        try {
            if (! $this->checkCollectionExists($this->projectionsTable)) {
                throw Exception\CollectionNotSetupException::with($this->projectionsTable);
            }
            $cursor = $this->collection($this->projectionsTable)->find(
                ['name' => ['$regex' => $filter]],
                [
                    'projection' => ['name' => 1],
                    'sort' => ['name' => 1],
                    'limit' => $limit,
                    'skip' => $offset,
                ]
            );
        } catch (MongoDbException $exception) {
            if ($exception->getCode() === 2) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        $projectionNames = [];

        foreach ($cursor as $projection) {
            $projectionNames[] = $projection['name'];
        }

        return $projectionNames;
    }

    public function fetchProjectionStatus(string $name): ProjectionStatus
    {
        try {
            $result = $this->collection($this->projectionsTable)->findOne(
                ['name' => $name],
                [
                    'projection' => ['status' => 1],
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        if (empty($result['status'])) {
            throw ProjectionNotFound::withName($name);
        }

        return ProjectionStatus::byValue($result['status']);
    }

    public function fetchProjectionStreamPositions(string $name): array
    {
        try {
            $result = $this->collection($this->projectionsTable)->findOne(
                ['name' => $name],
                [
                    'projection' => ['position' => 1],
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        if (! $result) {
            throw ProjectionNotFound::withName($name);
        }

        return $result['position'];
    }

    public function fetchProjectionState(string $name): array
    {
        try {
            $result = $this->collection($this->projectionsTable)->findOne(
                ['name' => $name],
                [
                    'projection' => ['state' => 1],
                ]
            );
        } catch (MongoDbException $exception) {
            throw Exception\RuntimeException::fromMongoDbException($exception);
        }

        if (! $result) {
            throw ProjectionNotFound::withName($name);
        }

        return $result['state'];
    }
}
