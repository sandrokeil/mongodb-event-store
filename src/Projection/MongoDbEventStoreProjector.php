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

use ArrayIterator;
use Closure;
use DateTimeImmutable;
use DateTimeZone;
use EmptyIterator;
use MongoDB\Client;
use MongoDB\Driver\Exception\Exception as MongoDbException;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\MongoDb\Exception\CollectionNotSetupException;
use Prooph\EventStore\MongoDb\Exception\ProjectionNotCreatedException;
use Prooph\EventStore\MongoDb\Exception\RuntimeException;
use Prooph\EventStore\MongoDb\MongoDbHelper;
use Prooph\EventStore\MongoDb\MongoEventStore;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\Util\ArrayCache;

final class MongoDbEventStoreProjector implements Projector
{
    use MongoDbHelper;
    use ProcessEvents;

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
    private $name;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    /**
     * @var array
     */
    private $streamPositions = [];

    /**
     * @var ArrayCache
     */
    private $cachedStreamNames;

    /**
     * @var int
     */
    private $persistBlockSize;

    /**
     * @var array
     */
    private $state = [];

    /**
     * @var ProjectionStatus
     */
    private $status;

    /**
     * @var callable|null
     */
    private $initCallback;

    /**
     * @var Closure|null
     */
    private $handler;

    /**
     * @var array
     */
    private $handlers = [];

    /**
     * @var boolean
     */
    private $isStopped = false;

    /**
     * @var ?string
     */
    private $currentStreamName = null;

    /**
     * @var int lock timeout in milliseconds
     */
    private $lockTimeoutMs;

    /**
     * @var int
     */
    private $eventCounter = 0;

    /**
     * @var int
     */
    private $sleep;

    /**
     * @var bool
     */
    private $triggerPcntlSignalDispatch;

    /**
     * @var int
     */
    private $updateLockThreshold;

    /**
     * @var array|null
     */
    private $query;

    /**
     * @var bool
     */
    private $streamCreated = false;

    /**
     * @var DateTimeImmutable
     */
    private $lastLockUpdate;

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
        string $name,
        string $eventStreamsTable,
        string $projectionsTable,
        int $lockTimeoutMs,
        int $cacheSize,
        int $persistBlockSize,
        int $sleep,
        bool $triggerPcntlSignalDispatch = false,
        int $updateLockThreshold = 0
    ) {
        if ($triggerPcntlSignalDispatch && ! \extension_loaded('pcntl')) {
            throw Exception\ExtensionNotLoadedException::withName('pcntl');
        }

        $this->eventStore = $eventStore;
        $this->client = $client;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->messageFactory = $messageFactory;
        $this->database = $database;
        $this->name = $name;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
        $this->lockTimeoutMs = $lockTimeoutMs;
        $this->cachedStreamNames = new ArrayCache($cacheSize);
        $this->persistBlockSize = $persistBlockSize;
        $this->sleep = $sleep;
        $this->status = ProjectionStatus::IDLE();
        $this->triggerPcntlSignalDispatch = $triggerPcntlSignalDispatch;
        $this->updateLockThreshold = $updateLockThreshold;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof MongoEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function init(Closure $callback): Projector
    {
        if (null !== $this->initCallback) {
            throw new Exception\RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (\is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName): Projector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;

        return $this;
    }

    public function fromStreams(string ...$streamNames): Projector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->query['streams'][] = $streamName;
        }

        return $this;
    }

    public function fromCategory(string $name): Projector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['categories'][] = $name;

        return $this;
    }

    public function fromCategories(string ...$names): Projector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($names as $name) {
            $this->query['categories'][] = $name;
        }

        return $this;
    }

    public function fromAll(): Projector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['all'] = true;

        return $this;
    }

    public function when(array $handlers): Projector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (! \is_string($eventName)) {
                throw new Exception\InvalidArgumentException('Invalid event name given, string expected');
            }

            if (! $handler instanceof Closure) {
                throw new Exception\InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    public function whenAny(Closure $handler): Projector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function emit(Message $event): void
    {
        if (! $this->streamCreated || ! $this->eventStore->hasStream(new StreamName($this->name))) {
            $this->eventStore->create(new Stream(new StreamName($this->name), new EmptyIterator()));
            $this->streamCreated = true;
        }

        $this->linkTo($this->name, $event);
    }

    public function linkTo(string $streamName, Message $event): void
    {
        $sn = new StreamName($streamName);

        if ($this->cachedStreamNames->has($streamName)) {
            $append = true;
        } else {
            $this->cachedStreamNames->rollingAppend($streamName);
            $append = $this->eventStore->hasStream($sn);
        }

        if ($append) {
            $this->eventStore->appendTo($sn, new ArrayIterator([$event]));
        } else {
            $this->eventStore->create(new Stream($sn, new ArrayIterator([$event])));
        }
    }

    public function reset(): void
    {
        $this->streamPositions = [];

        $callback = $this->initCallback;

        $this->state = [];

        if (\is_callable($callback)) {
            $result = $callback();

            if (\is_array($result)) {
                $this->state = $result;
            }
        }

        try {
            $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $this->name,
                ],
                [
                    '$set' => [
                        'position' => $this->streamPositions,
                        'state' => $this->state,
                        'status' => $this->status->getValue(),
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        try {
            $this->eventStore->delete(new StreamName($this->name));
        } catch (Exception\StreamNotFound $exception) {
            // ignore
        }
    }

    public function stop(): void
    {
        $this->persist();
        $this->isStopped = true;

        try {
            $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $this->name,
                ],
                [
                    '$set' => [
                        'status' => ProjectionStatus::IDLE()->getValue(),
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        $this->status = ProjectionStatus::IDLE();
    }

    public function getState(): array
    {
        return $this->state;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function delete(bool $deleteEmittedEvents): void
    {
        try {
            $this->collection($this->projectionsTable)->deleteOne(
                [
                    'name' => $this->name,
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if ($deleteEmittedEvents) {
            try {
                $this->eventStore->delete(new StreamName($this->name));
            } catch (Exception\StreamNotFound $e) {
                // ignore
            }
        }

        $this->isStopped = true;

        $callback = $this->initCallback;

        $this->state = [];

        if (\is_callable($callback)) {
            $result = $callback();

            if (\is_array($result)) {
                $this->state = $result;
            }
        }

        $this->streamPositions = [];
    }

    public function run(bool $keepRunning = true): void
    {
        if (null === $this->query
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new Exception\RuntimeException('No handlers configured');
        }

        switch ($this->fetchRemoteStatus()) {
            case ProjectionStatus::STOPPING():
                $this->stop();

                return;
            case ProjectionStatus::DELETING():
                $this->delete(false);

                return;
            case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS():
                $this->delete(true);

                return;
            case ProjectionStatus::RESETTING():
                $this->reset();
                break;
            default:
                break;
        }

        if (! $this->projectionExists()) {
            $this->createProjection();
        }

        $this->acquireLock();

        $this->prepareStreamPositions();
        $this->load();

        $singleHandler = null !== $this->handler;

        $this->isStopped = false;

        try {
            $this->processEvents($keepRunning, $singleHandler);
        } finally {
            $this->releaseLock();
        }
    }

    private function fetchRemoteStatus(): ProjectionStatus
    {
        try {
            $result = $this->collection($this->projectionsTable)->findOne(
                ['name' => $this->name],
                [
                    'projection' => ['status' => 1],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (empty($result['status'])) {
            if (! $this->checkCollectionExists($this->projectionsTable)) {
                throw CollectionNotSetupException::with($this->projectionsTable);
            }

            return ProjectionStatus::RUNNING();
        }

        return ProjectionStatus::byValue($result['status']);
    }

    private function createHandlerContext(?string &$streamName)
    {
        return new class($this, $streamName) {
            /**
             * @var Projector
             */
            private $projector;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(Projector $projector, ?string &$streamName)
            {
                $this->projector = $projector;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->projector->stop();
            }

            public function linkTo(string $streamName, Message $event): void
            {
                $this->projector->linkTo($streamName, $event);
            }

            public function emit(Message $event): void
            {
                $this->projector->emit($event);
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
    }

    private function load(): void
    {
        try {
            $result = $this->collection($this->projectionsTable)->findOne(
                ['name' => $this->name],
                [
                    'projection' => [
                        'state' => 1,
                        'position' => 1,
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        $this->streamPositions = \array_merge($this->streamPositions, $result['position']);

        if (! empty($result['state'])) {
            $this->state = $result['state'];
        }
    }

    private function projectionExists(): bool
    {
        try {
            $result = $this->collection($this->projectionsTable)->findOne(
                ['name' => $this->name],
                [
                    'projection' => [
                        'status' => 1,
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        return ! empty($result);
    }

    private function createProjection(): void
    {
        try {
            $this->collection($this->projectionsTable)->insertOne(
                [
                    'name' => $this->name,
                    'position' => new \stdClass(),
                    'state' => new \stdClass(),
                    'status' => $this->status->getValue(),
                    'locked_until' => null,
                ]
            );
        } catch (MongoDbException $exception) {
            throw ProjectionNotCreatedException::with($this->name);
        }
    }

    /**
     * @throws Exception\RuntimeException
     */
    private function acquireLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $nowString = $now->format('Y-m-d\TH:i:s.u');

        $lockUntilString = $this->createLockUntilString($now);

        try {
            $result = $this->collection($this->projectionsTable)->updateOne(
                [
                    '$and' => [
                        ['name' => $this->name],
                        [
                            '$or' => [
                                ['locked_until' => ['$eq' => null]],
                                ['locked_until' => ['$lt' => $nowString]],
                            ],
                        ],
                    ],
                ],
                [
                    '$set' => [
                        'locked_until' => $lockUntilString,
                        'status' => ProjectionStatus::RUNNING()->getValue(),
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getMatchedCount()) {
            if (! $this->checkCollectionExists($this->projectionsTable)) {
                throw CollectionNotSetupException::with($this->projectionsTable);
            }
            throw new Exception\RuntimeException('Another projection process is already running');
        }

        $this->status = ProjectionStatus::RUNNING();
        $this->lastLockUpdate = $now;
    }

    private function updateLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        if (! $this->shouldUpdateLock($now)) {
            return;
        }

        $lockUntilString = $this->createLockUntilString($now);

        try {
            $result = $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $this->name,
                ],
                [
                    '$set' => [
                        'locked_until' => $lockUntilString,
                        'position' => $this->streamPositions,
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getMatchedCount()) {
            if (! $this->checkCollectionExists($this->projectionsTable)) {
                throw CollectionNotSetupException::with($this->projectionsTable);
            }
            throw new Exception\RuntimeException('Unknown error occurred');
        }

        $this->lastLockUpdate = $now;
    }

    private function releaseLock(): void
    {
        try {
            $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $this->name,
                ],
                [
                    '$set' => [
                        'locked_until' => null,
                        'status' => ProjectionStatus::IDLE()->getValue(),
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        $this->status = ProjectionStatus::IDLE();
    }

    private function persist(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        $lockUntilString = $this->createLockUntilString($now);

        try {
            $this->collection($this->projectionsTable)->updateOne(
                [
                    'name' => $this->name,
                ],
                [
                    '$set' => [
                        'locked_until' => $lockUntilString,
                        'position' => $this->streamPositions,
                        'state' => $this->state,
                    ],
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }
    }

    private function prepareStreamPositions(): void
    {
        $streamPositions = [];

        if (isset($this->query['all'])) {
            try {
                $cursor = $this->collection($this->eventStreamsTable)->find(
                    [
                        'real_stream_name' => ['$regex' => '^(?!\$)'],
                    ],
                    [
                        'projection' => ['real_stream_name' => 1],
                    ]
                );
            } catch (MongoDbException $exception) {
                throw RuntimeException::fromMongoDbException($exception);
            }

            foreach ($cursor as $streamName) {
                $streamPositions[$streamName['real_stream_name']] = 0;
            }

            $this->streamPositions = \array_merge($streamPositions, $this->streamPositions);

            return;
        }

        if (isset($this->query['categories'])) {
            try {
                $cursor = $this->collection($this->eventStreamsTable)->find(
                    [
                        'category' => ['$in' => $this->query['categories']],
                    ],
                    [
                        'projection' => ['real_stream_name' => 1],
                    ]
                );
            } catch (MongoDbException $exception) {
                throw RuntimeException::fromMongoDbException($exception);
            }

            foreach ($cursor as $streamName) {
                $streamPositions[$streamName['real_stream_name']] = 0;
            }

            $this->streamPositions = \array_merge($streamPositions, $this->streamPositions);

            return;
        }

        // stream names given
        foreach ($this->query['streams'] as $streamName) {
            $streamPositions[$streamName] = 0;
        }

        $this->streamPositions = \array_merge($streamPositions, $this->streamPositions);
    }

    private function createLockUntilString(DateTimeImmutable $from): string
    {
        $micros = (string) ((int) $from->format('u') + ($this->lockTimeoutMs * 1000));

        $secs = \substr($micros, 0, -6);

        if ('' === $secs) {
            $secs = 0;
        }

        $resultMicros = \substr($micros, -6);

        return $from->modify('+' . $secs .' seconds')->format('Y-m-d\TH:i:s') . '.' . $resultMicros;
    }

    private function shouldUpdateLock(DateTimeImmutable $now): bool
    {
        if ($this->lastLockUpdate === null || $this->updateLockThreshold === 0) {
            return true;
        }

        $intervalSeconds = \floor($this->updateLockThreshold / 1000);

        //Create an interval based on seconds
        $updateLockThreshold = new \DateInterval("PT{$intervalSeconds}S");
        //and manually add split seconds
        $updateLockThreshold->f = ($this->updateLockThreshold % 1000) / 1000;

        $threshold = $this->lastLockUpdate->add($updateLockThreshold);

        return $threshold <= $now;
    }
}
