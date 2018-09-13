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

namespace ProophTest\EventStore\MongoDb\Container;

use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\MongoDb\Container\MongoDbProjectionManagerFactory;
use Prooph\EventStore\MongoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\MongoDb\MongoDbEventStore;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\MongoDb\Projection\MongoDbProjectionManager;
use ProophTest\EventStore\MongoDb\TestUtil;
use Psr\Container\ContainerInterface;

/**
 * @group Container
 */
class MongoDbProjectionManagerFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_service(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'client' => 'my client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = new MongoDbEventStore(
            $this->createMock(MessageFactory::class),
            $client,
            TestUtil::getDatabaseName(),
            $this->createMock(PersistenceStrategy::class)
        );

        $container->get('my client')->willReturn($client)->shouldBeCalled();
        $container->get(EventStore::class)->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $factory = new MongoDbProjectionManagerFactory();
        $projectionManager = $factory($container->reveal());

        $this->assertInstanceOf(MongoDbProjectionManager::class, $projectionManager);
    }

    /**
     * @test
     */
    public function it_creates_service_via_callstatic(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'client' => 'my client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = new MongoDbEventStore(
            $this->createMock(MessageFactory::class),
            $client,
            TestUtil::getDatabaseName(),
            $this->createMock(PersistenceStrategy::class)
        );

        $container->get('my client')->willReturn($client)->shouldBeCalled();
        $container->get(EventStore::class)->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $name = 'default';
        $pdo = MongoDbProjectionManagerFactory::$name($container->reveal());

        $this->assertInstanceOf(MongoDbProjectionManager::class, $pdo);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projectionName = 'custom';
        MongoDbProjectionManagerFactory::$projectionName('invalid container');
    }
}
