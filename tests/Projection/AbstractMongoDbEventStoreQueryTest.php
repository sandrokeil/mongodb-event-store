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
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\MongoDb\MongoDbEventStore;
use Prooph\EventStore\MongoDb\MongoDbHelper;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\MongoDb\Projection\MongoDbEventStoreQuery;
use Prooph\EventStore\MongoDb\Projection\MongoDbProjectionManager;
use Prooph\EventStore\Projection\ProjectionManager;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\MongoDb\TestUtil;
use ProophTest\EventStore\Projection\AbstractEventStoreQueryTest;

abstract class AbstractMongoDbEventStoreQueryTest extends AbstractEventStoreQueryTest
{
    use MongoDbHelper;

    /**
     * @var ProjectionManager
     */
    protected $projectionManager;

    /**
     * @var Client
     */
    protected $client;

    /**
     * @var string
     */
    protected $database;

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;

    protected function setUp(): void
    {
        $this->client = TestUtil::getClient();
        $this->database = TestUtil::getDatabaseName();

        self::createEventStreamsCollection($this->client, TestUtil::getDatabaseName(), 'event_streams');
        self::createProjectionCollection($this->client, TestUtil::getDatabaseName(), 'projections');

        $this->eventStore = new MongoDbEventStore(
            new FQCNMessageFactory(),
            $this->client,
            $this->database,
            $this->getPersistenceStrategy()
        );
        $this->projectionManager = new MongoDbProjectionManager(
            $this->eventStore,
            $this->client,
            $this->getPersistenceStrategy(),
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
     * @small
     */
    public function it_stops_immediately_after_pcntl_signal_was_received(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-long-running-query.php';
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        /**
         * Created process inherits env variables from this process.
         * Script returns with non-standard code SIGUSR1 from the handler and -1 else
         */
        $projectionProcess = \proc_open($command, $descriptorSpec, $pipes);
        $processDetails = \proc_get_status($projectionProcess);
        \usleep(500000);
        \posix_kill($processDetails['pid'], SIGQUIT);
        \usleep(500000);

        $processDetails = \proc_get_status($projectionProcess);
        $this->assertEquals(
            SIGUSR1,
            $processDetails['exitcode']
        );
    }

    /**
     * @test
     */
    public function it_updates_state_using_when_and_persists_with_block_size(): void
    {
        $this->prepareEventStream('user-123');

        $testCase = $this;

        $query = $this->projectionManager->createQuery();

        $query
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    return $state;
                },
                UsernameChanged::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    if ($event->payload()['name'] === 'Sascha') {
                        $this->stop();
                    }

                    return $state;
                },
            ])
            ->run();

        $this->assertEquals('Sascha', $query->getState()['name']);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown event store instance given');

        $eventStore = $this->prophesize(EventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new MongoDbEventStoreQuery(
            $wrappedEventStore->reveal(),
            $this->client,
            $this->database,
            'event_streams'
        );
    }

    /**
     * @test
     */
    public function it_throws_exception_when_unknown_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown event store instance given');

        $eventStore = $this->prophesize(EventStore::class);
        $client = $this->prophesize(Client::class);

        new MongoDbEventStoreQuery($eventStore->reveal(), $client->reveal(), $this->database, 'event_streams');
    }
}
