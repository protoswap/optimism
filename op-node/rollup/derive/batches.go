package derive

import (
	"errors"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-node/eth"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

var DifferentEpoch = errors.New("batch is of different epoch")

func FilterBatches(log log.Logger, config *rollup.Config, epoch rollup.Epoch, minL2Time uint64, maxL2Time uint64, batches []*BatchData) (out []*BatchData) {
	uniqueTime := make(map[uint64]struct{})
	for _, batch := range batches {
		if err := ValidBatch(batch, config, epoch, minL2Time, maxL2Time); err != nil {
			if err == DifferentEpoch {
				log.Trace("ignoring batch of different epoch", "epoch", batch.Epoch, "expected_epoch", epoch, "timestamp", batch.Timestamp, "txs", len(batch.Transactions))
			} else {
				log.Warn("filtered batch", "epoch", batch.Epoch, "timestamp", batch.Timestamp, "txs", len(batch.Transactions), "err", err)
			}
			continue
		}
		// Check if we have already seen a batch for this L2 block
		if _, ok := uniqueTime[batch.Timestamp]; ok {
			// block already exists, batch is duplicate (first batch persists, others are ignored)
			continue
		}
		uniqueTime[batch.Timestamp] = struct{}{}
		out = append(out, batch)
	}
	return
}

func FilterBatchesV2(config *rollup.Config, epoch rollup.Epoch, minL2Time uint64, maxL2Time uint64, batches []*BatchWithL1InclusionBlock) (out []*BatchWithL1InclusionBlock) {
	uniqueTime := make(map[uint64]struct{})
	for _, batch := range batches {
		if err := ValidBatch(batch.Batch, config, epoch, minL2Time, maxL2Time); err != nil {
			continue
		}
		// Check if we have already seen a batch for this L2 block
		if _, ok := uniqueTime[batch.Batch.Timestamp]; ok {
			// block already exists, batch is duplicate (first batch persists, others are ignored)
			continue
		}
		uniqueTime[batch.Batch.Timestamp] = struct{}{}
		out = append(out, batch)
	}
	return
}

func ValidBatch(batch *BatchData, config *rollup.Config, epoch rollup.Epoch, minL2Time uint64, maxL2Time uint64) error {
	if batch.Epoch != epoch {
		// Batch was tagged for past or future epoch,
		// i.e. it was included too late or depends on the given L1 block to be processed first.
		return DifferentEpoch
	}
	if (batch.Timestamp-config.Genesis.L2Time)%config.BlockTime != 0 {
		return fmt.Errorf("bad timestamp %d, not a multiple of the block time", batch.Timestamp)
	}
	if batch.Timestamp < minL2Time {
		return fmt.Errorf("old batch: %d < %d", batch.Timestamp, minL2Time)
	}
	// limit timestamp upper bound to avoid huge amount of empty blocks
	if batch.Timestamp >= maxL2Time {
		return fmt.Errorf("batch too far into future: %d > %d", batch.Timestamp, maxL2Time)
	}
	for i, txBytes := range batch.Transactions {
		if len(txBytes) == 0 {
			return fmt.Errorf("transaction data must not be empty, but tx %d is empty", i)
		}
		if txBytes[0] == types.DepositTxType {
			return fmt.Errorf("sequencers may not embed any deposits into batch data, but tx %d has one", i)
		}
	}
	return nil
}

// FillMissingBatches turns a collection of batches to the input batches for a series of blocks
func FillMissingBatches(batches []*BatchData, epoch, blockTime, minL2Time, nextL1Time uint64) []*BatchData {
	m := make(map[uint64]*BatchData)
	// The number of L2 blocks per sequencing window is variable, we do not immediately fill to maxL2Time:
	// - ensure at least 1 block
	// - fill up to the next L1 block timestamp, if higher, to keep up with L1 time
	// - fill up to the last valid batch, to keep up with L2 time
	newHeadL2Timestamp := minL2Time
	if nextL1Time > newHeadL2Timestamp+blockTime {
		newHeadL2Timestamp = nextL1Time - blockTime
	}
	for _, b := range batches {
		m[b.BatchV1.Timestamp] = b
		if b.Timestamp > newHeadL2Timestamp {
			newHeadL2Timestamp = b.Timestamp
		}
	}
	var out []*BatchData
	for t := minL2Time; t <= newHeadL2Timestamp; t += blockTime {
		b, ok := m[t]
		if ok {
			out = append(out, b)
		} else {
			out = append(out, &BatchData{
				BatchV1{
					Epoch:     rollup.Epoch(epoch),
					Timestamp: t,
				},
			})
		}
	}
	return out
}

// FillMissingBatches turns a collection of batches to the input batches for a series of blocks
func FillMissingBatchesV2(batches []*BatchWithL1InclusionBlock, epoch, blockTime, minL2Time, nextL1Time, baseBlockNum uint64) []*BatchWithL1InclusionBlock {
	m := make(map[uint64]*BatchWithL1InclusionBlock)
	// The number of L2 blocks per sequencing window is variable, we do not immediately fill to maxL2Time:
	// - ensure at least 1 block
	// - fill up to the next L1 block timestamp, if higher, to keep up with L1 time
	// - fill up to the last valid batch, to keep up with L2 time
	newHeadL2Timestamp := minL2Time
	if nextL1Time > newHeadL2Timestamp+blockTime {
		newHeadL2Timestamp = nextL1Time - blockTime
	}
	for _, b := range batches {
		m[b.Batch.Timestamp] = b
		if b.Batch.Timestamp > newHeadL2Timestamp {
			newHeadL2Timestamp = b.Batch.Timestamp
		}
	}
	var out []*BatchWithL1InclusionBlock
	for t := minL2Time; t <= newHeadL2Timestamp; t += blockTime {
		baseBlockNum += 1
		b, ok := m[t]
		if ok {
			out = append(out, b)
		} else {
			out = append(out, &BatchWithL1InclusionBlock{
				Batch: &BatchData{
					BatchV1{
						Epoch:       rollup.Epoch(epoch),
						Timestamp:   t,
						BlockNumber: baseBlockNum,
					},
				},

				L1InclusionBlock: eth.L1BlockRef{}, // TODO. Not really relevant. Maybe end of sequencing window...
			})
		}
	}
	return out
}
