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

namespace ProophTest\EventStore\MongoDb;

use ArrayIterator;
use MongoDB\Client;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\MongoDb\MongoDbEventStore;
use Prooph\EventStore\MongoDb\MongoDbHelper;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\AbstractEventStoreTest;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\TransactionalEventStoreTestTrait;

abstract class AbstractMongoDbEventStoreTest extends AbstractEventStoreTest
{
    use MongoDbHelper;

    use TransactionalEventStoreTestTrait;

    /**
     * @var Client
     */
    protected $client;

    /**
     * @var string
     */
    protected $database;

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;

    protected function setUp()
    {
        $this->client = TestUtil::getClient();

        self::createEventStreamsCollection($this->client, TestUtil::getDatabaseName(), 'event_streams');

        $this->eventStore = new MongoDbEventStore(
            new FQCNMessageFactory(),
            $this->client,
            TestUtil::getDatabaseName(),
            $this->getPersistenceStrategy()
        );
    }

    protected function tearDown()
    {
        TestUtil::tearDownDatabase();
    }

    public function it_throws_on_invalid_field_for_message_property(): void
    {
        $this->markTestSkipped('How to check this in schema less world?');
    }

    /**
     * Was overridden because field are different e.g. uuid = event_id and createdAt = created_at
     *
     * @test
     */
    public function it_returns_only_matched_metadata(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);
        $uuid = $event->uuid()->toString();
        $before = $event->createdAt()->modify('-5 secs')->format('Y-m-d\TH:i:s.u');
        $later = $event->createdAt()->modify('+5 secs')->format('Y-m-d\TH:i:s.u');
        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$event]));
        $this->eventStore->create($stream);
        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::EQUALS(), 'bar');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::NOT_EQUALS(), 'baz');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::GREATER_THAN(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int2', Operator::GREATER_THAN_EQUALS(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::IN(), [4, 5, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int3', Operator::LOWER_THAN(), 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int4', Operator::LOWER_THAN_EQUALS(), 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::NOT_IN(), [4, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::REGEX(), '^b[a]r$');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_id', Operator::EQUALS(), $uuid, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_id', Operator::NOT_EQUALS(), 'baz', FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::GREATER_THAN(), $before, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::GREATER_THAN_EQUALS(), $before, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_id', Operator::IN(), [$uuid, 2, 3], FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::LOWER_THAN(), $later, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::LOWER_THAN_EQUALS(), $later, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::NOT_IN(), [$before, $later], FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_name', Operator::REGEX(), '.+UserCreated$', FieldType::MESSAGE_PROPERTY());
        $streamEvents = $this->eventStore->load($stream->streamName(), 1, null, $metadataMatcher);
        $this->assertCount(1, $streamEvents);
    }

    /**
     * Was overridden because field are different e.g. uuid = event_id and createdAt = created_at
     *
     * @test
     */
    public function it_returns_only_matched_metadata_reverse(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);
        $uuid = $event->uuid()->toString();
        $before = $event->createdAt()->modify('-5 secs')->format('Y-m-d\TH:i:s.u');
        $later = $event->createdAt()->modify('+5 secs')->format('Y-m-d\TH:i:s.u');
        $streamName = new StreamName('Prooph\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]));
        $this->eventStore->create($stream);
        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::EQUALS(), 'bar');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::NOT_EQUALS(), 'baz');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::GREATER_THAN(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int2', Operator::GREATER_THAN_EQUALS(), 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::IN(), [4, 5, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int3', Operator::LOWER_THAN(), 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int4', Operator::LOWER_THAN_EQUALS(), 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::NOT_IN(), [4, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::REGEX(), '^b[a]r$');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_id', Operator::EQUALS(), $uuid, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_id', Operator::NOT_EQUALS(), 'baz', FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::GREATER_THAN(), $before, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::GREATER_THAN_EQUALS(), $before, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_id', Operator::IN(), [$uuid, 2, 3], FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::LOWER_THAN(), $later, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::LOWER_THAN_EQUALS(), $later, FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('created_at', Operator::NOT_IN(), [$before, $later], FieldType::MESSAGE_PROPERTY());
        $metadataMatcher = $metadataMatcher->withMetadataMatch('event_name', Operator::REGEX(), '.+UserCreated$', FieldType::MESSAGE_PROPERTY());
        $streamEvents = $this->eventStore->loadReverse($stream->streamName(), 1, null, $metadataMatcher);
        $this->assertCount(1, $streamEvents);
    }
}
