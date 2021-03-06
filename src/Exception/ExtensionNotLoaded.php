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

class ExtensionNotLoaded extends RuntimeException
{
    public static function with(string $extensionNme): ExtensionNotLoaded
    {
        return new self(\sprintf('Extension "%s" is not loaded', $extensionNme));
    }
}
