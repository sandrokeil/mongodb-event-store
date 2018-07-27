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
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\Exception\ConfigurationException;
use Prooph\EventStore\Metadata\MetadataEnricher;
use Prooph\EventStore\MongoDb\Container\MongoDbEventStoreFactory;
use Prooph\EventStore\MongoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\MongoDb\MongoDbEventStore;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\Plugin\Plugin;
use Prooph\EventStore\TransactionalActionEventEmitterEventStore;
use ProophTest\EventStore\MongoDb\TestUtil;
use Psr\Container\ContainerInterface;

/**
 * @group Container
 */
final class MongoDbEventStoreFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter_via_connection_service(): void
    {
        $config['prooph']['event_store']['default'] = [
            'client' => 'my_client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
            'wrap_action_event_emitter' => false,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_client')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $factory = new MongoDbEventStoreFactory();
        $eventStore = $factory($container->reveal());

        $this->assertInstanceOf(MongoDbEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_wraps_action_event_emitter(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'client' => 'my_client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_client')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $eventStoreName = 'custom';
        $eventStore = MongoDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(TransactionalActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_injects_plugins(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'client' => 'my_client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
            'plugins' => ['plugin'],
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_client')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $featureMock = $this->getMockForAbstractClass(Plugin::class);
        $featureMock->expects($this->once())->method('attachToEventStore');

        $container->get('plugin')->willReturn($featureMock);

        $eventStoreName = 'custom';
        $eventStore = MongoDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_plugin_configured(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Plugin plugin does not implement the Plugin interface');

        $config['prooph']['event_store']['custom'] = [
            'client' => 'my_client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
            'plugins' => ['plugin'],
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_client')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $container->get('plugin')->willReturn('notAValidPlugin');

        $eventStoreName = 'custom';
        MongoDbEventStoreFactory::$eventStoreName($container->reveal());
    }

    /**
     * @test
     */
    public function it_injects_metadata_enrichers(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'client' => 'my_client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
            'metadata_enrichers' => ['metadata_enricher1', 'metadata_enricher2'],
        ];

        $metadataEnricher1 = $this->prophesize(MetadataEnricher::class);
        $metadataEnricher2 = $this->prophesize(MetadataEnricher::class);

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config);
        $container->get('my_client')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $container->get('metadata_enricher1')->willReturn($metadataEnricher1->reveal());
        $container->get('metadata_enricher2')->willReturn($metadataEnricher2->reveal());

        $eventStoreName = 'custom';
        $eventStore = MongoDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_metadata_enricher_configured(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Metadata enricher foobar does not implement the MetadataEnricher interface');

        $config['prooph']['event_store']['custom'] = [
            'client' => 'my_client',
            'database' => 'test_db',
            'persistence_strategy' => PersistenceStrategy\MongoDbAggregateStreamStrategy::class,
            'metadata_enrichers' => ['foobar'],
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config);
        $container->get('my_client')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\MongoDbAggregateStreamStrategy::class)->willReturn($this->prophesize(PersistenceStrategy::class))->shouldBeCalled();

        $container->get('foobar')->willReturn('foobar');

        $eventStoreName = 'custom';
        MongoDbEventStoreFactory::$eventStoreName($container->reveal());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStoreName = 'custom';
        MongoDbEventStoreFactory::$eventStoreName('invalid container');
    }
}
