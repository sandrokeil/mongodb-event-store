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

namespace Prooph\EventStore\MongoDb;

use Countable;
use Iterator;
use MongoDB\Client;
use MongoDB\Driver\Exception\Exception as MongoDbException;
use MongoDB\Driver\Session;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\MongoDb\Exception\ExtensionNotLoaded;
use Prooph\EventStore\MongoDb\Exception\RuntimeException;
use Prooph\EventStore\MongoDb\PersistenceStrategy\MongoDbAggregateStreamStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\TransactionalEventStore;
use Prooph\EventStore\Util\Assertion;

final class MongoDbEventStore implements MongoEventStore, TransactionalEventStore
{
    use MongoDbHelper;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var Client
     */
    private $client;

    /**
     * The transaction id, if currently in transaction, otherwise null
     *
     * @var Session|null
     */
    private $session;

    /**
     * @var string
     */
    private $database;

    /**
     * @var PersistenceStrategy
     */
    private $persistenceStrategy;

    /**
     * @var int
     */
    private $loadBatchSize;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var bool
     */
    private $disableTransactionHandling;

    /**
     *
     *
     * @var array
     */
    private $createdCollections = [];

    /**
     * @throws ExtensionNotLoaded
     */
    public function __construct(
        MessageFactory $messageFactory,
        Client $client,
        string $database,
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams',
        bool $disableTransactionHandling = false
    ) {
        if (! \extension_loaded('mongodb')) {
            throw ExtensionNotLoaded::with('mongodb');
        }

        Assertion::min($loadBatchSize, 1);

        $this->messageFactory = $messageFactory;
        $this->client = $client;
        $this->database = $database;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->disableTransactionHandling = $disableTransactionHandling;
    }

    public function fetchStreamMetadata(StreamName $streamName): array
    {
        $options = ['projection' => ['metadata' => 1]];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        try {
            $stream = $this->collection($this->eventStreamsTable)->findOne(
                ['real_stream_name' => $streamName->toString()],
                $options
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (! $stream) {
            throw StreamNotFound::with($streamName);
        }

        return $stream['metadata'];
    }

    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        $options = [];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        try {
            $result = $this->collection($this->eventStreamsTable)->updateOne(
                ['real_stream_name' => $streamName->toString()],
                ['$set' => ['metadata' => $newMetadata]],
                $options
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getMatchedCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    public function hasStream(StreamName $streamName): bool
    {
        $options = [];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        try {
            $number = $this->collection($this->eventStreamsTable)->countDocuments(
                ['real_stream_name' => $streamName->toString()],
                  $options
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        return 1 === $number;
    }

    public function create(Stream $stream): void
    {
        $streamName = $stream->streamName();
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        try {
            $this->createSchemaFor($collectionName);
            $this->addStreamToStreamsTable($stream);
        } catch (RuntimeException $exception) {
            $this->collection($collectionName)->drop();
            $this->removeStreamFromStreamsTable($streamName);

            throw $exception;
        } catch (StreamExistsAlready $exception) {
            throw $exception;
        }

        $this->appendTo($streamName, $stream->streamEvents());
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $tableName = $this->persistenceStrategy->generateCollectionName($streamName);
        if (! $this->checkCollectionExists($tableName)) {
            throw StreamNotFound::with($streamName);
        }
        $nextSequence = 0;
        if (! $this->persistenceStrategy instanceof MongoDbAggregateStreamStrategy) {
            $number = $streamEvents instanceof Countable ? $streamEvents->count() : \iterator_count($streamEvents);

            if ($number === 0) {
                return;
            }

            $nextSequence = $this->nextSequence($tableName, $number);
        }

        $data = $this->persistenceStrategy->prepareData($streamEvents, $nextSequence);

        if (empty($data)) {
            return;
        }

        $options = [];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        try {
            $this->collection($tableName)->insertMany($data, $options);
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }
    }

    public function load(
        StreamName $streamName,
        int $fromNumber = 1,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        $tableName = $this->persistenceStrategy->generateCollectionName($streamName);

        try {
            $data = $this->collection($this->eventStreamsTable)->findOne(
                ['stream_name' => $tableName],
                ['projection' => ['stream_name' => 1]]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (empty($data)) {
            throw StreamNotFound::with($streamName);
        }

        $where = $this->createWhereClause($metadataMatcher);
        $where[]['_id'] = ['$gte' => $fromNumber];

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = \min($count, $this->loadBatchSize);
        }

        $collection = $this->collection($tableName);

        $options = [
            'sort' => ['_id' => 1],
            'limit' => $limit,
        ];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        return new MongoDbStreamIterator(
            function (array $filter = [], array $innerOptions = []) use ($collection, $options, $where) {
                $innerOptions = \array_replace_recursive(
                    $options,
                    $innerOptions
                );
                \array_walk($where, function (array &$value) use ($filter) {
                    if (isset($value['_id'], $filter['_id'])) {
                        $value['_id'] = $filter['_id'];
                    }
                });

                return $collection->find(
                    ['$and' => $where],
                    $innerOptions
                );
            },
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            true
        );
    }

    public function loadReverse(
        StreamName $streamName,
        int $fromNumber = null,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        if (null === $fromNumber) {
            $fromNumber = PHP_INT_MAX;
        }
        $tableName = $this->persistenceStrategy->generateCollectionName($streamName);

        try {
            $data = $this->collection($this->eventStreamsTable)->findOne(
                ['stream_name' => $tableName],
                ['projection' => ['stream_name' => 1]]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (empty($data)) {
            throw StreamNotFound::with($streamName);
        }

        $where = $this->createWhereClause($metadataMatcher);
        $where[]['_id'] = ['$lte' => $fromNumber];

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = \min($count, $this->loadBatchSize);
        }

        $collection = $this->collection($tableName);

        $options = [
            'sort' => ['_id' => -1],
            'limit' => $limit,
        ];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        return new MongoDbStreamIterator(
            function (array $filter = [], array $innerOptions = []) use ($collection, $options, $where) {
                $innerOptions = \array_replace_recursive(
                    $options,
                    $innerOptions
                );
                \array_walk($where, function (array &$value) use ($filter) {
                    if (isset($value['_id'], $filter['_id'])) {
                        $value['_id'] = $filter['_id'];
                    }
                });

                return $collection->find(
                    ['$and' => $where],
                    $innerOptions
                );
            },
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            false
        );
    }

    public function delete(StreamName $streamName): void
    {
        $this->removeStreamFromStreamsTable($streamName);

        $encodedStreamName = $this->persistenceStrategy->generateCollectionName($streamName);

        try {
            $this->collection($encodedStreamName)->drop();
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }
    }

    public function beginTransaction(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if (null !== $this->session) {
            throw new TransactionAlreadyStarted();
        }
        $this->session = $this->client->startSession();
        $this->session->startTransaction([
            'readConcern' => new \MongoDB\Driver\ReadConcern('snapshot'),
            'writeConcern' => new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY),
        ]);
    }

    public function commit(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if (null === $this->session) {
            throw new TransactionNotStarted();
        }

        $this->session->commitTransaction();
        $this->session->endSession();
        $this->session = null;
        $this->createdCollections = [];
    }

    public function rollback(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if (null === $this->session) {
            throw new TransactionNotStarted();
        }
        $this->session->abortTransaction();
        $this->session->endSession();

        foreach ($this->createdCollections as $collectionName) {
            $this->collection($collectionName)->drop();
        }

        $this->session = null;
        $this->createdCollections = [];
    }

    public function fetchStreamNames(
        ?string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array {
        $where = $this->createWhereClause($metadataMatcher);

        if (null !== $filter) {
            $where[]['real_stream_name'] = $filter;
        }

        if (! empty($where)) {
            $where = ['$and' => $where];
        }

        try {
            $cursor = $this->collection($this->eventStreamsTable)->find(
                $where,
                [
                    'projection' => ['real_stream_name' => 1],
                    'sort' => ['real_stream_name' => 1],
                    'limit' => $limit,
                    'skip' => $offset,
                ]
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        $streamNames = [];

        foreach ($cursor as $streamName) {
            $streamNames[] = new StreamName($streamName['real_stream_name']);
        }

        return $streamNames;
    }

    public function fetchStreamNamesRegex(
        string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array {
        $where = $this->createWhereClause($metadataMatcher);
        $where[]['real_stream_name'] = ['$regex' => $filter];

        try {
            $cursor = $this->collection($this->eventStreamsTable)->find(
                ['$and' => $where],
                [
                    'projection' => ['real_stream_name' => 1],
                    'sort' => ['real_stream_name' => 1],
                    'limit' => $limit,
                    'skip' => $offset,
                ]
            );
        } catch (MongoDbException $exception) {
            if ($exception->getCode() === 2) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            throw RuntimeException::fromMongoDbException($exception);
        }

        $streamNames = [];

        foreach ($cursor as $streamName) {
            $streamNames[] = new StreamName($streamName['real_stream_name']);
        }

        return $streamNames;
    }

    public function fetchCategoryNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        if (null !== $filter) {
            $where['category'] = $filter;
        } else {
            $where['category'] = ['$ne' => null];
        }

        try {
            $cursor = $this->collection($this->eventStreamsTable)->aggregate([
                ['$match' => $where],
                ['$group' => ['_id' => '$category']],
                ['$sort' => ['_id' => 1]],
                ['$skip' => $offset],
                ['$limit' => $limit],
            ]);
        } catch (MongoDbException $exception) {
            if ($exception->getCode() === 2) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            throw RuntimeException::fromMongoDbException($exception);
        }

        $categoryNames = [];

        foreach ($cursor as $categoryName) {
            $categoryNames[] = $categoryName['_id'];
        }

        return $categoryNames;
    }

    public function fetchCategoryNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        $where['category'] = ['$regex' => $filter];

        try {
            $cursor = $this->collection($this->eventStreamsTable)->aggregate([
                ['$match' => $where],
                ['$group' => ['_id' => '$category']],
                ['$sort' => ['category' => 1]],
                ['$skip' => $offset],
                ['$limit' => $limit],
            ]);
        } catch (MongoDbException $exception) {
            if ($exception->getCode() === 2) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            throw RuntimeException::fromMongoDbException($exception);
        }

        $categoryNames = [];

        foreach ($cursor as $categoryName) {
            $categoryNames[] = $categoryName['_id'];
        }

        return $categoryNames;
    }

    private function createWhereClause(?MetadataMatcher $metadataMatcher): array
    {
        $where = [];

        if (! $metadataMatcher) {
            return $where;
        }

        foreach ($metadataMatcher->data() as $key => $match) {
            /** @var FieldType $fieldType */
            $fieldType = $match['fieldType'];
            $field = $match['field'];
            /** @var Operator $operator */
            $operator = $match['operator'];
            $value = $match['value'];
            $fieldPrefix = '';

            if ($fieldType->is(FieldType::METADATA())) {
                $fieldPrefix = 'metadata.';
            }

            switch ($operator->getValue()) {
                case Operator::EQUALS:
                    $operator = '$eq';
                    break;
                case Operator::GREATER_THAN:
                    $operator = '$gt';
                    break;
                case Operator::GREATER_THAN_EQUALS:
                    $operator = '$gte';
                    break;
                case Operator::LOWER_THAN:
                    $operator = '$lt';
                    break;
                case Operator::LOWER_THAN_EQUALS:
                    $operator = '$lte';
                    break;
                case Operator::NOT_EQUALS:
                    $operator = '$ne';
                    if ($value === null) {
                        $value = ['$exists' => true];
                    } else {
                        $where[][$fieldPrefix . $field] = ['$exists' => true];
                    }
                    break;
                case Operator::REGEX:
                    $operator = '$regex';
                    break;
                case Operator::IN:
                    $operator = '$in';
                    break;
                case Operator::NOT_IN:
                    $operator = '$nin';
                    break;
                default:
                    break;
            }

            $value = (array) $value;

            if ($operator === '$in' || $operator === '$nin') {
                $where[][$fieldPrefix . $field] = [$operator => $value];
            } else {
                foreach ($value as $item) {
                    $where[][$fieldPrefix . $field] = [$operator => $item];
                }
            }
        }

        return $where;
    }

    private function addStreamToStreamsTable(Stream $stream): void
    {
        $realStreamName = $stream->streamName()->toString();

        $pos = \strpos($realStreamName, '-');

        if (false !== $pos && $pos > 0) {
            $category = \substr($realStreamName, 0, $pos);
        } else {
            $category = null;
        }

        $options = [];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }

        try {
            $this->collection($this->eventStreamsTable)->insertOne(
                [
                    'real_stream_name' => $realStreamName,
                    'stream_name' => $this->persistenceStrategy->generateCollectionName($stream->streamName()),
                    'metadata' => $stream->metadata(),
                    'category' => $category,
                ],
                $options
            );
        } catch (MongoDbException $exception) {
            if ($exception->getCode() === 11000) {
                throw StreamExistsAlready::with($stream->streamName());
            }
            throw RuntimeException::fromMongoDbException($exception);
        }
    }

    private function removeStreamFromStreamsTable(StreamName $streamName): void
    {
        $options = [];

        if (! $this->disableTransactionHandling && null !== $this->session) {
            $options = [
                'session' => $this->session,
            ];
        }
        try {
            $result = $this->collection($this->eventStreamsTable)->deleteOne(
                [
                    'real_stream_name' => $streamName->toString(),
                ],
                $options
            );
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }

        if (1 !== $result->getDeletedCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    private function createSchemaFor(string $collectionName): void
    {
        $schema = $this->persistenceStrategy->createSchema($collectionName);

        try {
            if (! $this->persistenceStrategy instanceof MongoDbAggregateStreamStrategy) {
                $this->initializeCounter($collectionName);
            }
            $this->collection($collectionName)->createIndexes($schema);
            $this->createdCollections[] = $collectionName;
        } catch (MongoDbException $exception) {
            if ($exception->getCode() === 11000) {
                throw StreamExistsAlready::with(new StreamName($collectionName));
            }
            throw RuntimeException::fromMongoDbException($exception);
        }
    }

    private function initializeCounter(string $collectionName): void
    {
        $this->collection('counter')->insertOne(
            [
                '_id' => $collectionName,
                'seq' => 1,
            ]
        );
    }

    private function nextSequence(string $collectionName, $inc = 1): int
    {
        $result = $this->collection('counter')->findOneAndUpdate(
            [
                '_id' => $collectionName,
            ],
            [
                '$inc' => ['seq' => $inc],
            ],
            [
                'returnDocument' => \MongoDB\Operation\FindOneAndUpdate::RETURN_DOCUMENT_AFTER,
            ]
        );

        return $result['seq'] - $inc;
    }

    public function inTransaction(): bool
    {
        return null !== $this->session;
    }

    public function transactional(callable $callable)
    {
        $this->beginTransaction();

        try {
            $result = $callable($this);
            $this->commit();
        } catch (\Throwable $e) {
            $this->rollback();
            throw $e;
        }

        return $result ?: true;
    }
}
