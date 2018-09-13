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

namespace ProophTest\EventStore\MongoDb\Projection;

use MongoDB\Client;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\MongoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\MongoDb\Exception\RuntimeException;
use Prooph\EventStore\MongoDb\MongoDbEventStore;
use Prooph\EventStore\MongoDb\MongoDbHelper;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\MongoDb\Projection\MongoDbProjectionManager;
use Prooph\EventStore\Projection\InMemoryProjectionManager;
use ProophTest\EventStore\MongoDb\TestUtil;
use ProophTest\EventStore\Projection\AbstractProjectionManagerTest;

/**
 * @group ProjectionManager
 * @group Projection
 */
class MongoDbProjectionManagerTest extends AbstractProjectionManagerTest
{
    use MongoDbHelper;

    /**
     * @var MongoDbProjectionManager
     */
    protected $projectionManager;

    /**
     * @var MongoDbEventStore
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

    protected function setUp(): void
    {
        $this->client = TestUtil::getClient();
        $this->database = TestUtil::getDatabaseName();

        self::createEventStreamsCollection($this->client, TestUtil::getDatabaseName(), 'event_streams');
        self::createProjectionCollection($this->client, TestUtil::getDatabaseName(), 'projections');

        $persistenceStrategy = $this->prophesize(PersistenceStrategy::class)->reveal();

        $this->eventStore = new MongoDbEventStore(
            new FQCNMessageFactory(),
            $this->client,
            $this->database,
            $persistenceStrategy
        );
        $this->projectionManager = new MongoDbProjectionManager(
            $this->eventStore,
            $this->client,
            $persistenceStrategy,
            new FQCNMessageFactory(),
            $this->database
        );
    }

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);

        new InMemoryProjectionManager($eventStore->reveal());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(EventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();
        $persistenceStrategy = $this->prophesize(PersistenceStrategy::class)->reveal();

        new MongoDbProjectionManager(
            $wrappedEventStore->reveal(),
            $this->client,
            $persistenceStrategy,
            new FQCNMessageFactory(),
            TestUtil::getDatabaseName()
        );
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->collection('projections')->drop();
        $this->projectionManager->fetchProjectionNames(null, 200, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_using_invalid_regex(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid regex pattern given');

        $this->projectionManager->fetchProjectionNamesRegex('invalid)', 10, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_regex_with_missing_db_table(): void
    {
        $this->expectException(RuntimeException::class);

        $this->collection('projections')->drop();
        $this->projectionManager->fetchProjectionNamesRegex('^foo', 200, 0);
    }
}
