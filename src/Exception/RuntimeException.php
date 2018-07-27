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

namespace Prooph\EventStore\MongoDb\Exception;

use MongoDB\Driver\Exception\Exception as MongoDbException;
use Prooph\EventStore\Exception\RuntimeException as EventStoreRuntimeException;

class RuntimeException extends EventStoreRuntimeException implements MongoDbEventStoreException
{
    public static function fromMongoDbException(MongoDbException $mongoDbException): RuntimeException
    {
        return new self(
            $mongoDbException->getMessage(),
            $mongoDbException->getCode(),
            $mongoDbException
        );
    }

    public static function streamExist(string $streamName): RuntimeException
    {
        return new self(\sprintf('Stream collection "%s" already exists.', $streamName));
    }
}
