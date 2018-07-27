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

namespace Prooph\EventStore\MongoDb\PersistenceStrategy;

use Iterator;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\EventStore\MongoDb\DefaultMessageConverter;
use Prooph\EventStore\MongoDb\Exception;
use Prooph\EventStore\MongoDb\PersistenceStrategy;
use Prooph\EventStore\StreamName;

final class MongoDbSingleStreamStrategy implements PersistenceStrategy
{
    /**
     * @var MessageConverter
     */
    private $messageConverter;

    public function __construct(?MessageConverter $messageConverter = null)
    {
        $this->messageConverter = $messageConverter ?? new DefaultMessageConverter();
    }

    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array
    {
        return [
            [
                'key' => [
                    'metadata._aggregate_type' => 1,
                    'metadata._aggregate_id' => 1,
                    'metadata._aggregate_version' => 1,
                ],
                'unique' => true,
                'name' => 'aggregate',
                'background' => true,
            ],
            [
                'key' => ['event_id' => 1],
                'unique' => true,
                'name' => 'event_id',
                'background' => true,
            ],
        ];
    }

    public function columnNames(): array
    {
        return [
            'event_id',
            'event_name',
            'payload',
            'metadata',
            'created_at',
        ];
    }

    public function prepareData(Iterator $streamEvents, int &$no): array
    {
        $data = [];

        foreach ($streamEvents as $event) {
            $eventData = $this->messageConverter->convertToArray($event);

            if (! isset($eventData['metadata']['_aggregate_version'])) {
                throw new Exception\RuntimeException('_aggregate_version is missing in metadata');
            }
            $data[] = [
                '_id' => $no,
                'event_id' => $eventData['uuid'],
                'event_name' => $eventData['message_name'],
                'payload' => $eventData['payload'],
                'metadata' => $eventData['metadata'],
                'created_at' => $eventData['created_at']->format('Y-m-d\TH:i:s.u'),
            ];
            $no++;
        }

        return $data;
    }

    public function generateCollectionName(StreamName $streamName): string
    {
        return '_' . \sha1($streamName->toString());
    }
}
