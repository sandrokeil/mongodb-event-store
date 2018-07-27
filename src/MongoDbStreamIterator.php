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

use DateTimeImmutable;
use DateTimeZone;
use Generator;
use Iterator;
use MongoDB\Driver\Cursor;
use MongoDB\Exception\Exception as MongoDbException;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\MongoDb\Exception\RuntimeException;

final class MongoDbStreamIterator implements Iterator
{
    /**
     * @var callable
     */
    private $query;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var \stdClass|false
     */
    private $currentItem = null;

    /**
     * @var int
     */
    private $currentKey = -1;

    /**
     * @var int
     */
    private $batchPosition = 0;

    /**
     * @var int
     */
    private $batchSize;

    /**
     * @var int
     */
    private $fromNumber;

    /**
     * @var int
     */
    private $currentFromNumber;

    /**
     * @var int|null
     */
    private $count;

    /**
     * @var bool
     */
    private $forward;

    /**
     * @var Generator
     */
    private $generator;

    public function __construct(
        callable $query,
        MessageFactory $messageFactory,
        int $batchSize,
        int $fromNumber,
        ?int $count,
        bool $forward
    ) {
        $this->query = $query;
        $this->messageFactory = $messageFactory;
        $this->batchSize = $batchSize;
        $this->fromNumber = $fromNumber;
        $this->currentFromNumber = $fromNumber;
        $this->count = $count;
        $this->forward = $forward;

        $this->next();
    }

    /**
     * @return null|Message
     */
    public function current(): ?Message
    {
        if (null === $this->currentItem) {
            return null;
        }

        $createdAt = $this->currentItem['created_at'];

        if (\strlen($createdAt) === 19) {
            $createdAt = $createdAt . '.000';
        }

        $createdAt = DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $createdAt,
            new DateTimeZone('UTC')
        );

        $metadata = $this->currentItem['metadata'];

        if (! \array_key_exists('_position', $metadata)) {
            $metadata['_position'] = $this->currentItem['_id'];
        }

        $payload = $this->currentItem['payload'];

        return $this->messageFactory->createMessageFromArray($this->currentItem['event_name'], [
            'uuid' => $this->currentItem['event_id'],
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata,
        ]);
    }

    public function next(): void
    {
        if ($this->count && ($this->count - 1) === $this->currentKey) {
            $this->currentKey = -1;
            $this->currentItem = null;

            return;
        }
        $this->ensureCursor();
        if ($this->currentKey !== -1) {
            $this->generator->next();
        }

        $this->currentItem = $this->generator->current();

        if (null !== $this->currentItem) {
            $this->currentKey++;
            $this->currentFromNumber = $this->currentItem['_id'];

            return;
        }
        $this->batchPosition++;

        if ($this->forward) {
            $from = $this->currentFromNumber + 1;
        } else {
            $from = $this->currentFromNumber - 1;
        }
        $this->buildStatement($from);

        $this->currentItem = $this->generator->current();

        if (null === $this->currentItem) {
            $this->currentKey = -1;
        } else {
            $this->currentKey++;
            $this->currentFromNumber = $this->currentItem['_id'];
        }
    }

    /**
     * @return bool|int
     */
    public function key()
    {
        if (null === $this->currentItem) {
            return false;
        }

        return $this->currentItem['_id'];
    }

    /**
     * @return bool
     */
    public function valid(): bool
    {
        return null !== $this->currentItem;
    }

    public function rewind(): void
    {
        //Only perform rewind if current item is not the first element
        if ($this->currentKey !== 0) {
            $this->batchPosition = 0;
            $this->generator = null;
            $this->buildStatement($this->fromNumber);

            $this->currentItem = null;
            $this->currentKey = -1;
            $this->currentFromNumber = $this->fromNumber;

            $this->next();
        }
    }

    private function ensureCursor(array $filter = [], array $options = []): void
    {
        if (null === $this->generator) {
            $callable = $this->query;

            try {
                $this->generator = $this->yieldCursor($callable($filter, $options));
            } catch (MongoDbException $exception) {
                throw RuntimeException::fromMongoDbException($exception);
            }
        }
    }

    private function buildStatement(int $fromNumber): void
    {
        if (null === $this->count
            || $this->count < ($this->batchSize * ($this->batchPosition + 1))
        ) {
            $limit = $this->batchSize;
        } else {
            $limit = $this->count - ($this->batchSize * $this->batchPosition);
        }

        $this->ensureCursor(
            [
                '_id' => [$this->forward ? '$gte' : '$lte' => $fromNumber],
            ],
            [
                'limit' => $limit,
            ]
        );
    }

    private function yieldCursor(Cursor $cursor): Generator
    {
        foreach ($cursor as $item) {
            yield $item;
        }
    }
}
