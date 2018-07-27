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

/**
 * @group ReadModel
 * @group Projection
 * @group AggregateStream
 */
class MongoDbEventStoreReadModelProjectorAggregateStreamStrategyTest extends AbstractMongoDbEventStoreReadModelProjectorTest
{
    protected function getPersistenceStrategy(): PersistenceStrategy
    {
        return new PersistenceStrategy\MongoDbAggregateStreamStrategy();
    }
}
