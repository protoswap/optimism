package derive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/eth"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

type L1ReceiptsFetcher interface {
	Fetch(ctx context.Context, blockHash common.Hash) (eth.L1Info, types.Transactions, types.Receipts, error)
}

type BatchWithOrigin struct {
	Origin eth.L1BlockRef
	Batch  *BatchData
}

// BatchQueue contains a set of batches for every L1 block.
// L1 blocks are contiguous and this does not support reorgs.
type BatchQueue struct {
	// originComplete bool // false if the last origin expects more batches
	// lastL1Origin   eth.L1BlockRef

	log    log.Logger
	config *rollup.Config
	dl     L1ReceiptsFetcher

	// Window of L1 blocks that are in place. Bounded to SeqWindow Size + processing delta
	window []eth.L1BlockRef

	// TODO: Accessory function / data for the 0th and 1st element of the window
	// They are often used as "L1Origin" and "NextL1Block" for setting timestamp bounds.

	// epoch number that the batch queue is currently processing
	// IMPT: This must be updated in lockstep with the window
	epoch uint64

	// All batches with the same L2 block number. Batches are ordered by when they are seen.
	// Do a linear scan over the batches rather than deeply nested maps.
	// Note: Only a single batch with the same tuple (block number, timestamp, epoch) is allowed.
	batchesByNumber map[uint64][]*BatchWithOrigin
}

// NewBatchQueue creates a BatchQueue, which should be Reset(origin) before use.
func NewBatchQueue(log log.Logger, cfg *rollup.Config, dl L1ReceiptsFetcher) *BatchQueue {
	return &BatchQueue{
		log:             log,
		config:          cfg,
		dl:              dl,
		batchesByNumber: make(map[uint64][]*BatchWithOrigin),
	}
}

// LastL1Origin returns the L1 Block with the highest number that the BatchQueue knows of
func (bq *BatchQueue) LastL1Origin() eth.L1BlockRef {
	last := eth.L1BlockRef{}
	if len(bq.window) != 0 {
		last = bq.window[len(bq.window)-1]
	}
	return last
}

// AddOrigin adds an Origin to the BatchQueue.
func (bq *BatchQueue) AddOrigin(l1Block eth.L1BlockRef) error {
	parent := bq.LastL1Origin()
	if parent.Hash != l1Block.ParentHash {
		return fmt.Errorf("cannot process L1 reorg from %s to %s (parent %s)", parent.ID(), l1Block.ID(), l1Block.ParentID())
	}
	bq.window = append(bq.window, l1Block)
	return nil
}

func (bq *BatchQueue) AddBatch(batch *BatchData, origin eth.L1BlockRef) error {
	data := BatchWithOrigin{
		Origin: origin,
		Batch:  batch,
	}
	batches, ok := bq.batchesByNumber[batch.BlockNumber]
	// Filter complete duplicates. This step is not strictly needed as we always append, but it is nice to avoid lots
	// of spam. If we enforced a tighter relationship between block number & timestamp we could limit to a single
	// blocknumber & epoch pair.
	if ok {
		for _, b := range batches {
			if b.Batch.BlockNumber == batch.BlockNumber &&
				b.Batch.Timestamp == batch.Timestamp &&
				b.Batch.Epoch == batch.Epoch {
				return errors.New("duplicate batch")
			}
		}
	}
	// May have duplicate block numbers or individual fields, but have limitted complete duplicates
	bq.batchesByNumber[batch.BlockNumber] = append(batches, &data)
	return nil
}

func (bq *BatchQueue) EndOrigin() {
	// bq.originComplete = true
}

// validExtension determines if a batch follows the previous attributes
func validExtension(cfg *rollup.Config, batch *BatchWithOrigin, prevNumber, prevTime, prevEpoch uint64) bool {
	if batch.Batch.BlockNumber != prevNumber+1 {
		return false
	}
	if batch.Batch.Timestamp != prevTime+cfg.BlockTime {
		return false
	}
	if batch.Batch.Epoch != rollup.Epoch(prevEpoch) && batch.Batch.Epoch != rollup.Epoch(prevEpoch+1) {
		return false
	}
	// TODO (VERY IMPORTANT): Filter this batch out if the origin of the batch is too far past the epoch
	// TODO (ALSO VERY IMPT): Get this equality check correct
	// TODO: Also do this check when ingesting batches.
	if batch.Origin.Number > uint64(batch.Batch.Epoch)+cfg.SeqWindowSize {
		return false
	}
	return true
}

// allFollowingBatches pulls as many batches as possible from the window & saved batches
// All batches are validExtensions of the prior batch & the first batch is a validExtension
// of the l2SafeHead. All batchs pass the `ValidBatch` check (or are created empty batches).
//
// This function works by running a loop of the following:
// 	1. Eagerly pull batches with `followingBatches`
//	2. Fill out empty batches if it must.
//	3. Repeat until no more batches can be created.
func (bq *BatchQueue) allFollowingBatches(l2SafeHead *eth.L2BlockRef) []*BatchWithOrigin {
	var ret []*BatchWithOrigin
	lastL2Timestamp := l2SafeHead.Time

	for {
		// 1. Get following batches with an eager rule.
		following := bq.followingBatches(l2SafeHead)
		foundFollowing := len(following) != 0
		ret = append(ret, following...)

		// 2. Decide if need to fill out empty batches
		fillEmpty := bq.LastL1Origin().Number > bq.epoch+bq.config.SeqWindowSize
		if fillEmpty {
			// 2a. Gather all batches. First sort by number and then by first seen.
			var bns []uint64
			for n := range bq.batchesByNumber {
				bns = append(bns, n)
			}
			sort.Slice(bns, func(i, j int) bool { return bns[i] < bns[j] })

			var possibleBatches []*BatchWithOrigin
			for _, n := range bns {
				for _, batch := range bq.batchesByNumber[n] {
					if batch.Batch.Epoch == rollup.Epoch(bq.epoch) {
						possibleBatches = append(possibleBatches, batch)
					}
				}
			}

			// 2b. Determine the valid time window
			l1OriginTime := bq.window[0].Time
			nextL1BlockTime := bq.window[1].Time
			if len(ret) > 0 {
				lastL2Timestamp = ret[len(ret)-1].Batch.Timestamp
			}
			minL2Time := lastL2Timestamp + bq.config.BlockTime
			maxL2Time := l1OriginTime + bq.config.MaxSequencerDrift
			if minL2Time+bq.config.BlockTime > maxL2Time {
				maxL2Time = minL2Time + bq.config.BlockTime
			}
			// Filter + Fill batches
			possibleBatches = FilterBatchesV2(bq.config, rollup.Epoch(bq.epoch), minL2Time, maxL2Time, possibleBatches)
			possibleBatches = FillMissingBatchesV2(possibleBatches, bq.epoch, bq.config.BlockTime, minL2Time, nextL1BlockTime)

			// Advance an epoch after filling all batches.
			ret = append(ret, possibleBatches...)
			bq.epoch += 1
			bq.window = bq.window[1:]

		}

		// 3. If not more batches could be found, exit.
		if !foundFollowing && !fillEmpty {
			return ret
		}
	}
}

// followingBatches returns a list of batches that each are valid extensions of each other
// where the first batch is a valid extension of the l2SafeHead.
// It may return an empty list.
func (bq *BatchQueue) followingBatches(l2SafeHead *eth.L2BlockRef) []*BatchWithOrigin {
	var ret []*BatchWithOrigin

	prevNumber := l2SafeHead.Number
	prevTime := l2SafeHead.Number
	prevEpoch := l2SafeHead.L1Origin.Number

	for {
		// We require at least 1 L1 blocks to look at.
		if len(bq.window) == 0 {
			break
		}
		batches, ok := bq.batchesByNumber[prevNumber+1]
		// No more batches found. Rely on another function to fill missing batches for us.
		if !ok {
			break // break main loop
		}

		found := false
		for _, batch := range batches {
			l1OriginTime := bq.window[0].Time

			// If this batch advances the epoch, check it's validity against the next L1 Origin
			if batch.Batch.Epoch != rollup.Epoch(prevEpoch) {
				// With only 1 l1Block we cannot look at the next L1 Origin.
				// Note: This means that we are unable to determine validity of a batch
				// without more information. In this case we should bail out until we have
				// more information otherwise the eager algorithm may diverge from a non-eager
				// algorithm.
				if len(bq.window) < 2 {
					return ret
				}
				l1OriginTime = bq.window[1].Time
			}

			// Timestamp bounds
			minL2Time := prevTime + bq.config.BlockTime
			maxL2Time := l1OriginTime + bq.config.MaxSequencerDrift
			if minL2Time+bq.config.BlockTime > maxL2Time {
				maxL2Time = minL2Time + bq.config.BlockTime
			}

			// Note: Don't check epoch here, check it in `validExtensiohn`
			// Mainly check tx validity + timestamp bounds
			if err := ValidBatch(batch.Batch, bq.config, batch.Batch.Epoch, minL2Time, maxL2Time); err != nil {
				break
			}

			if validExtension(bq.config, batch, prevNumber, prevTime, prevEpoch) {
				// Advance the epoch
				if prevEpoch != uint64(batch.Batch.Epoch) {
					bq.window = bq.window[1:]
					bq.epoch = uint64(batch.Batch.Epoch)
				}
				// Don't leak data in the map
				delete(bq.batchesByNumber, batch.Batch.BlockNumber)

				prevNumber = batch.Batch.BlockNumber
				prevTime = batch.Batch.Timestamp
				prevEpoch = uint64(batch.Batch.Epoch)
				ret = append(ret, batch)

				found = true
				break // break this inner loop to continue the main loop
			}
		}
		// If there was no valid batch, make sure we properly break the main loop.
		if !found {
			break
		}
	}

	// At this point ret contains the set of all following batches that are all validExtensions of the l2Head or each other
	return ret
}

// derive any L2 chain inputs, if we have any new batches
func (bq *BatchQueue) DeriveL2Inputs(ctx context.Context, l2SafeHead eth.L2BlockRef) ([]*eth.PayloadAttributes, error) {
	if len(bq.window) == 0 {
		return nil, io.EOF
	}
	oldEpoch := bq.epoch
	oldWindow := make([]eth.L1BlockRef, len(bq.window))
	copy(oldWindow, bq.window)

	batches := bq.allFollowingBatches(&l2SafeHead)
	// Note have batches, but `bq.epoch` and `bq.window` have been modified.

	epoch := oldEpoch
	seqNumber := l2SafeHead.SequenceNumber + 1
	originIdx := 0
	l1Origin := oldWindow[0]

	var attributes []*eth.PayloadAttributes

	for _, batch := range batches {
		var l1Info eth.L1Info
		var receipts []*types.Receipt
		var deposits []hexutil.Bytes

		if epoch != uint64(batch.Batch.Epoch) {
			seqNumber = 0
			originIdx += 1
			l1Origin = oldWindow[originIdx]
			fetchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			var err error
			l1Info, _, receipts, err = bq.dl.Fetch(fetchCtx, l1Origin.Hash)
			if err != nil {
				bq.log.Error("failed to fetch L1 block info", "l1Origin", l1Origin, "err", err)
				panic("will lose data without reset")
				// TODO: return actual error here
				return nil, nil
			}
			var errs []error
			deposits, errs = DeriveDeposits(receipts, bq.config.DepositContractAddress)
			for _, err := range errs {
				bq.log.Error("Failed to derive a deposit", "l1OriginHash", l1Origin.Hash, "err", err)
			}
			if len(errs) != 0 {
				panic("will lose data without reset")
				return nil, fmt.Errorf("failed to derive some deposits: %v", errs)
			}
		}

		var txns []eth.Data
		l1InfoTx, err := L1InfoDepositBytes(seqNumber, l1Info)
		if err != nil {
			panic("will lose data without reset")
			return nil, fmt.Errorf("failed to create l1InfoTx: %w", err)
		}
		txns = append(txns, l1InfoTx)
		if seqNumber == 0 {
			txns = append(txns, deposits...)
		}
		txns = append(txns, batch.Batch.Transactions...)
		attrs := &eth.PayloadAttributes{
			Timestamp:             hexutil.Uint64(batch.Batch.Timestamp),
			PrevRandao:            eth.Bytes32(l1Info.MixDigest()),
			SuggestedFeeRecipient: bq.config.FeeRecipientAddress,
			Transactions:          txns,
			// we are verifying, not sequencing, we've got all transactions and do not pull from the tx-pool
			// (that would make the block derivation non-deterministic)
			NoTxPool: true,
		}
		attributes = append(attributes, attrs) // TODO: direct assignment here

		seqNumber += 1
	}
	return attributes, nil
}

func (bq *BatchQueue) Reset(l1Origin eth.L1BlockRef) {
	bq.window = bq.window[:0]
	// TODO: Is this the correct place to set the epoch to?
	bq.epoch = l1Origin.Number
	bq.batchesByNumber = make(map[uint64][]*BatchWithOrigin)
}
