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

use Iterator;
use Prooph\EventStore\StreamName;

interface PersistenceStrategy
{
    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array;

    public function columnNames(): array;

    public function prepareData(Iterator $streamEvents, int &$no): array;

    public function generateCollectionName(StreamName $streamName): string;
}
