# Batch submitting

The process that signs and publishes transactions is not part of the rollup node, but an external “batcher”.
This is effectively the inverse function of the derivation process, but specified here for posterity. This API design is not enforced by the protocol.

The batcher may use any data-availability platform that registers batch data within a chain of block-hashes and block-numbers.

The rollup node is mostly agnostic to the data platform: as a verifier the channel-bank has to traverse the chain and data has to be written to the bank.
Ethereum calldata is supported by default, other data-sources may be supported in future upgrades.

Between the batcher and rollup node there exists a simple single-method RPC:

- method: `optimism_batchData`
- params:
  1. object, with fields:
     - `history`: a dictionary of any previous submitted channel IDs (key, `DATA`) and the last frame number (value, `QUANTITY`) that was submitted for these.
     - `min_size`: `QUANTITY`, the minimum amount of data that may be returned. (TODO)
     - `max_size`: `QUANTITY`, the maximum amount of data that may be returned. For the batcher to configure, to fit whatever the data host can process at a time.
     - `max_blocks_per_channel`: `QUANTITY`, the maximum number of blocks to put in the same frame together.
- returns:
   1. `channels`: a dictionary of channels ID keys (`DATA`) of the channels that had frame(s) in the output `data`, with the last used frame number of the channel as value (`QUANTITY`).
   2. `data`: `DATA`, the encoded data, no larger than `max_size`.


The returned `channels` will be empty if there was no data to submit.

It is up to the batcher to track an aggregate of all returned `channels`, remove the old channels by timing them out, and submitting them as `history` for the next request.

The rollup node tracks outgoing opened channels with the following data each:

- `blocks`: List of block IDs (block hash and block number) that have to be encoded as batches in this channel
- `frame`: the frame number to use for the next output.
- `offset`: to track how much data has been pulled from the channel reader yet.
- `created`: the timestamp of the L1 block head at the time of the creation of the channel.
- `reader`: the reader that the remaining data of the channel can be read from. `nil` when closed.

The rollup node processes the request as follows:

1. Remove all previously opened (through previous requests) channels that have timed out, compared to the current L1 head block timestamp: `channel.created + CHANNEL_TIMEOUT < l1_head.timestamp`
2. Get a set `unsafe_blocks` (hash and number only) of at most the first 1000 unsafe blocks since the safe L2 block (the block derived from L1 data).
3. Iterate over all of the `request.history`, `channel_id, frame_number` each:
    1. Stop if we run out of time to add more frames to the output.
    2. Stop if there is not enough space left for another frame in the output
    3. Find if we know the open channel, continue to the next entry if not.
    4. If the `frame_number + 1` is not the `channel.frame`, then remove `channel.blocks` from the `unsafe_blocks` set, and continue with the next history entry.
        1. A request with an earlier frame number than the last would create a duplicate frame for the same channel, this is better to avoid since the last data can still be replayed. If the previous submission is not replayed, then the channel has to be timed out before the data can be submitted again.
            1. TODO: we can allow the requester to drop open channels, to avoid the wait for timeout.
        2. A request with a future frame number as history is erroneous, and the channel is better avoided.
    5. If the channel is closed (`reader == nil`), then remove `channel.blocks` from the `unsafe_blocks` set, and continue with the next history entry.
    6. Try to produce the next frame of the open channel, specifying the remaining available space as `max_size` for the frame.
    7. Add the channel ID and new frame number to the response.
4. After completing any open channel(s), use the remaining space to submit any `unsafe_blocks` that were not already removed from the set, ordered by increasing block number. Iterate until we run out of blocks or space:
    1. Stop if we run out of time to add more frames to the output.
    2. Stop if there is not enough space left for another frame in the output
    3. Create a new random `channel_id`
    4. Sanity check if the `channel_id` exists as open channel, skip if yes.
    5. Select a slice of `blocks` from the remaining `unsafe_blocks`, at most `max_blocks_per_channel`
    6. Create a new channel with the `channel_id`, `blocks`, `frame=0`, `offset=0`, `created=l1_head.timestamp`, and a new reader to pull the transformed `blocks` data from. Register the channel in the rollup node as open channel for future request handling.
    7. Use the channel to produce a new frame, specifying the remaining available space as `max_size` for the frame.
