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

use MongoDB\Client;
use MongoDB\Collection;

trait MongoDbHelper
{
    private function collection(string $collectionName): Collection
    {
        return $this->client->selectCollection(
            $this->database,
            $collectionName,
            [
                'typeMap' => [
                    'root' => 'array',
                    'document' => 'array',
                    'array' => 'array',
                ],
            ]
        );
    }

    public static function createEventStreamsCollection(Client $client, string $database, string $eventStreamsTable): void
    {
        $client->selectDatabase($database)->selectCollection($eventStreamsTable)->createIndex(
            [
                'stream_name' => 1,
            ],
            [
                'unique' => true,
            ]
        );
    }

    public static function createProjectionCollection(Client $client, string $database, string $projectionTable): void
    {
        $client->selectDatabase($database)->selectCollection($projectionTable)->createIndex(
            [
                'name' => 1,
            ],
            [
                'unique' => true,
            ]
        );
    }

    private function checkCollectionExists(string $collectionName): bool
    {
        return ! empty(
            \iterator_to_array($this->client->selectDatabase($this->database)->listCollections(['filter' => ['name' => $collectionName]]))
        );
    }
}
