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

use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;

/**
 * @group Projector
 * @group Projection
 * @group SimpleStream
 */
class MongoDbEventStoreProjectorSimpleStreamStrategyTest extends AbstractMongoDbEventStoreProjectorTest
{
    protected function getPersistenceStrategy(): PersistenceStrategy
    {
        return new PersistenceStrategy\MongoDbSimpleStreamStrategy();
    }

    /**
     * @test
     */
    public function it_changes_to_mongodb_change_stream(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $events = [];

        for ($i = 1; $i < 21; $i++) {
            $events[] = UserCreated::with([
                'id' => $i,
                'time' => \microtime(true),
            ], 1);
        }

        $this->eventStore->create(new Stream(new StreamName('user-123'), new \ArrayIterator($events)));

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-change-stream-projection.php';
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

        $result = null;
        while ($result === null) {
            \usleep(1000000);
            $result = $this->client->selectCollection($this->database, 'projections')->findOne();
        }
        $this->assertTrue($result['position']['user-123'] < 10);

        $this->eventStore->appendTo(
            new StreamName('user-123'),
            new \ArrayIterator([
                UserCreated::with([
                    'id' => 21,
                    'time' => \microtime(true),
                ], 1),
                UserCreated::with([
                    'id' => 22,
                    'time' => \microtime(true),
                ], 1),
                UserCreated::with([
                    'id' => 23,
                    'time' => \microtime(true),
                ], 1),
            ])
        );
        $result = $this->client->selectCollection($this->database, 'projections')->findOne();
        $this->assertTrue($result['position']['user-123'] < 10);

        \sleep(2);
        $result = $this->client->selectCollection($this->database, 'projections')->findOne();
        $this->assertSame(23, $result['position']['user-123']);
        $this->assertCount(23, $result['state']['aggregate_versions']);
        $this->assertSame(20, $result['state']['aggregate_versions'][19]);
        $this->assertSame(21, $result['state']['aggregate_versions'][20]);
        $this->assertSame(22, $result['state']['aggregate_versions'][21]);
        $this->assertSame(23, $result['state']['aggregate_versions'][22]);

        \sleep(1);
        $processDetails = \proc_get_status($projectionProcess);
        $this->assertEquals(
            SIG_DFL,
            $processDetails['exitcode']
        );
    }
}
