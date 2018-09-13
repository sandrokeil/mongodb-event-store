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

use MongoDB\Client;
use MongoDB\Exception\RuntimeException;

abstract class TestUtil
{
    /**
     * @var Client
     */
    private static $client;

    public static function getClient(): Client
    {
        if (self::$client === null) {
            $clientParams = self::getConnectionParams();

            $retries = 10; // keep trying for 10 seconds, should be enough
            while (null === self::$client && $retries > 0) {
                try {
                    self::$client = new Client(
                        $clientParams['uri'],
                        $clientParams['uriOptions'],
                        ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
                    );
                } catch (RuntimeException $e) {
                    $retries--;
                    \sleep(1);
                }
            }
        }

        if (! self::$client) {
            print "db connection could not be established. aborting...\n";
            exit(1);
        }

        return self::$client;
    }

    public static function getDatabaseName(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return \getenv('DB_NAME');
    }

    public static function getUriOptions(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getConnectionParams()['uriOptions'];
    }

    public static function getConnectionParams(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    public static function tearDownDatabase(): void
    {
        $client = self::getClient();
        $client->dropDatabase(self::getDatabaseName());
    }

    public static function getProjectionLockedUntilFromDefaultProjectionsTable(Client $client, string $projectionName): ?\DateTimeImmutable
    {
        $data = $client->selectDatabase(self::getDatabaseName())->selectCollection('projections')->findOne(
            ['name' => $projectionName],
            ['projection' => ['locked_until' => 1]]
        );

        if (! empty($data['locked_until'])) {
            return \DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s.u', $data['locked_until'], new \DateTimeZone('UTC'));
        }

        return null;
    }

    public static function subMilliseconds(\DateTimeImmutable $time, int $ms): \DateTimeImmutable
    {
        //Create a 0 interval
        $interval = new \DateInterval('PT0S');
        //and manually add split seconds
        $interval->f = $ms / 1000;

        return $time->sub($interval);
    }

    private static function hasRequiredConnectionParams(): bool
    {
        $env = \getenv();

        return isset(
            $env['DB_URI'],
            $env['DB_NAME'],
            $env['DB_REPLICA_SET']
        );
    }

    private static function getSpecifiedConnectionParams(): array
    {
        return [
            'uri' => \getenv('DB_URI'),
            'dbName' => \getenv('DB_NAME'),
            'uriOptions' => [
                'replicaSet' => \getenv('DB_REPLICA_SET'),
            ],
        ];
    }
}
