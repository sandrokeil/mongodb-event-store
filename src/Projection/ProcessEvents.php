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

namespace Prooph\EventStore\MongoDb\Projection;

use Prooph\Common\Messaging\Message;
use Prooph\EventStore\Exception;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\StreamName;

trait ProcessEvents
{
    private function processEvents(bool $keepRunning, bool $singleHandler): void
    {
        do {
            $collectionNames = [];
            $streamTimestampsStart = [];

            foreach ($this->streamPositions as $streamName => $position) {
                $collectionNames[$this->persistenceStrategy->generateCollectionName(new StreamName($streamName))] = $streamName;
            }
            // initialize stream to get new events during processing current events
            $changeStream = $this->client->selectDatabase($this->database)->watch([
                [
                    '$match' => [
                        'ns.coll' => ['$in' => \array_keys($collectionNames)],
                        'operationType' => 'insert',
                    ],
                ],
            ]);

            foreach ($this->streamPositions as $streamName => $position) {
                try {
                    $streamTimestampsStart[$streamName] = \time();
                    $streamEvents = $this->eventStore->load(new StreamName($streamName), $position + 1);
                } catch (Exception\StreamNotFound $e) {
                    // ignore
                    continue;
                }

                if ($singleHandler) {
                    $this->handleStreamWithSingleHandler($streamName, $streamEvents);
                } else {
                    $this->handleStreamWithHandlers($streamName, $streamEvents);
                }

                if ($this->isStopped) {
                    break;
                }
            }

            if (0 === $this->eventCounter) {
                \usleep($this->sleep);
                $this->updateLock();
            } else {
                $this->persist();
            }

            $this->eventCounter = 0;

            if ($this->triggerPcntlSignalDispatch) {
                \pcntl_signal_dispatch();
            }

            switch ($this->fetchRemoteStatus()) {
                case ProjectionStatus::STOPPING():
                    $this->stop();
                    break;
                case ProjectionStatus::DELETING():
                    $this->delete(false);
                    break;
                case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS():
                    $this->delete(true);
                    break;
                case ProjectionStatus::RESETTING():
                    $this->reset();
                    break;
                default:
                    break;
            }

            if (! $this->isStopped && $this->status === ProjectionStatus::RUNNING()) {
                for ($changeStream->rewind(); true; $changeStream->next()) {
                    if (! $changeStream->valid()) {
                        \usleep($this->sleep);
                        $this->updateLock();
                        if ($this->triggerPcntlSignalDispatch) {
                            \pcntl_signal_dispatch();
                        }
                        switch ($this->fetchRemoteStatus()) {
                            case ProjectionStatus::STOPPING():
                                $this->stop();
                                break;
                            case ProjectionStatus::DELETING():
                                $this->delete(false);
                                break;
                            case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS():
                                $this->delete(true);
                                break;
                            case ProjectionStatus::RESETTING():
                                $this->reset();
                                break;
                            default:
                                break;
                        }

                        if ($this->isStopped || ! $keepRunning) {
                            break;
                        }
                        continue;
                    }

                    $event = $changeStream->current();

                    if ($event['operationType'] === 'invalidate') {
                        break;
                    }

                    $streamName = $collectionNames[$event['ns']['coll']];
                    $eventTimestamp = $event['clusterTime']->getTimestamp();

                    // event already processed
                    if ($eventTimestamp <= $streamTimestampsStart[$streamName]
                        && $event['fullDocument']['_id'] <= $this->streamPositions[$streamName]
                    ) {
                        $this->updateLock();
                        continue;
                    }

                    $streamEvents = new \ArrayIterator([$event['fullDocument']['_id'] => $this->createMessage($event['fullDocument'])]);

                    if ($singleHandler) {
                        $this->handleStreamWithSingleHandler($streamName, $streamEvents);
                    } else {
                        $this->handleStreamWithHandlers($streamName, $streamEvents);
                    }
                    $this->persist();

                    if ($this->triggerPcntlSignalDispatch) {
                        \pcntl_signal_dispatch();
                    }

                    switch ($this->fetchRemoteStatus()) {
                        case ProjectionStatus::STOPPING():
                            $this->stop();
                            break;
                        case ProjectionStatus::DELETING():
                            $this->delete(false);
                            break;
                        case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS():
                            $this->delete(true);
                            break;
                        case ProjectionStatus::RESETTING():
                            $this->reset();
                            break;
                        default:
                            break;
                    }

                    if ($this->isStopped) {
                        break;
                    }
                }
            }

            $this->prepareStreamPositions();
        } while ($keepRunning && ! $this->isStopped);
    }

    private function createMessage(array $document): ?Message
    {
        $createdAt = $document['created_at'];

        if (\strlen($createdAt) === 19) {
            $createdAt .= '.000';
        }

        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $createdAt,
            new \DateTimeZone('UTC')
        );

        $metadata = $document['metadata'];

        if (! \array_key_exists('_position', $metadata)) {
            $metadata['_position'] = $document['_id'];
        }

        $payload = $document['payload'];

        return $this->messageFactory->createMessageFromArray($document['event_name'], [
            'uuid' => $document['event_id'],
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata,
        ]);
    }
}
