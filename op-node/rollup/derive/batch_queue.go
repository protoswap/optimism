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
	if batch.Origin.Number > batch.Origin.Number+cfg.SeqWindowSize {
		return false
	}
	return true
}

// allFollowingBatches pulls a single batch eagerly or a collection of batches if it is the end of
// the sequencing window.
func (bq *BatchQueue) allFollowingBatches(l2SafeHead *eth.L2BlockRef) []*BatchWithOrigin {

	// Decide if need to fill out empty batches & process an epoch at once
	// If not, just return a single batch
	if bq.LastL1Origin().Number >= bq.epoch+bq.config.SeqWindowSize {
		// 2a. Gather all batches. First sort by number and then by first seen.
		var bns []uint64
		for n := range bq.batchesByNumber {
			bns = append(bns, n)
		}
		sort.Slice(bns, func(i, j int) bool { return bns[i] < bns[j] })

		var batches []*BatchWithOrigin
		for _, n := range bns {
			for _, batch := range bq.batchesByNumber[n] {
				// Filter out batches that were submitted too late.
				// TODO: Another place to check the equality symbol.
				if batch.Origin.Number-bq.LastL1Origin().Number >= bq.config.SeqWindowSize {
					continue
				}
				// Pre filter batches in the correct epoch
				if batch.Batch.Epoch == rollup.Epoch(bq.epoch) {
					batches = append(batches, batch)
				}
			}
		}

		// 2b. Determine the valid time window
		l1OriginTime := bq.window[0].Time
		nextL1BlockTime := bq.window[1].Time
		minL2Time := l2SafeHead.Time + bq.config.BlockTime
		maxL2Time := l1OriginTime + bq.config.MaxSequencerDrift
		if minL2Time+bq.config.BlockTime > maxL2Time {
			maxL2Time = minL2Time + bq.config.BlockTime
		}
		// Filter + Fill batches
		batches = FilterBatchesV2(bq.config, rollup.Epoch(bq.epoch), minL2Time, maxL2Time, batches)
		batches = FillMissingBatchesV2(batches, bq.epoch, bq.config.BlockTime, minL2Time, nextL1BlockTime)

		// Advance an epoch after filling all batches.
		bq.epoch += 1
		bq.window = bq.window[1:]

		return batches

	} else {
		var ret []*BatchWithOrigin
		next := bq.followingBatches(l2SafeHead)
		if next != nil {
			ret = append(ret, next)
		}
		return ret
	}

}

// followingBatches returns a list of batches that each are valid extensions of each other
// where the first batch is a valid extension of the l2SafeHead.
// It may return an empty list.
func (bq *BatchQueue) followingBatches(l2SafeHead *eth.L2BlockRef) *BatchWithOrigin {

	// We require at least 1 L1 blocks to look at.
	if len(bq.window) == 0 {
		return nil
	}
	batches, ok := bq.batchesByNumber[l2SafeHead.Number+1]
	// No more batches found. Rely on another function to fill missing batches for us.
	if !ok {
		return nil
	}

	// Find the first batch saved for this number.
	// Note that we expect the number of batches for the same block number to be small (frequently just 1 ).
	for _, batch := range batches {
		l1OriginTime := bq.window[0].Time

		// If this batch advances the epoch, check it's validity against the next L1 Origin
		if batch.Batch.Epoch != rollup.Epoch(l2SafeHead.L1Origin.Number) {
			// With only 1 l1Block we cannot look at the next L1 Origin.
			// Note: This means that we are unable to determine validity of a batch
			// without more information. In this case we should bail out until we have
			// more information otherwise the eager algorithm may diverge from a non-eager
			// algorithm.
			if len(bq.window) < 2 {
				return nil
			}
			l1OriginTime = bq.window[1].Time
		}

		// Timestamp bounds
		minL2Time := l2SafeHead.Time + bq.config.BlockTime
		maxL2Time := l1OriginTime + bq.config.MaxSequencerDrift
		if minL2Time+bq.config.BlockTime > maxL2Time {
			maxL2Time = minL2Time + bq.config.BlockTime
		}

		// Note: Don't check epoch here, check it in `validExtension`
		// Mainly check tx validity + timestamp bounds
		if err := ValidBatch(batch.Batch, bq.config, batch.Batch.Epoch, minL2Time, maxL2Time); err != nil {
			break
		}

		// We have a valid batch, no make sure that it builds off the previous L2 block
		if validExtension(bq.config, batch, l2SafeHead.Number, l2SafeHead.Number, l2SafeHead.L1Origin.Number) {
			// Advance the epoch if needed
			if l2SafeHead.L1Origin.Number != uint64(batch.Batch.Epoch) {
				bq.window = bq.window[1:]
				bq.epoch = uint64(batch.Batch.Epoch)
			}
			// Don't leak data in the map
			delete(bq.batchesByNumber, batch.Batch.BlockNumber)

			// We have found the fist valid batch.
			return batch
		}
	}

	return nil
}

// derive any L2 chain inputs, if we have any new batches
func (bq *BatchQueue) DeriveL2Inputs(ctx context.Context, l2SafeHead eth.L2BlockRef) ([]*BatchWithOrigin, error) {
	if len(bq.window) == 0 {
		return nil, io.EOF
	}
	return bq.allFollowingBatches(&l2SafeHead), nil
}

func (bq *BatchQueue) Reset(l1Origin eth.L1BlockRef) {
	bq.window = bq.window[:0]
	// TODO: Is this the correct place to set the epoch to?
	bq.epoch = l1Origin.Number
	bq.batchesByNumber = make(map[uint64][]*BatchWithOrigin)
}

type PayloadQueue struct {
	log    log.Logger
	config *rollup.Config
	dl     L1ReceiptsFetcher

	l1Origins []eth.L1BlockRef
	batches   []*BatchWithOrigin
}

func (pq *PayloadQueue) AddBatches(batches []*BatchWithOrigin) {
	pq.batches = append(pq.batches, batches...)
}

func (pq *PayloadQueue) AddL1Origin(origin eth.L1BlockRef) {
	pq.l1Origins = append(pq.l1Origins, origin)
}

func (pq *PayloadQueue) DeriveL2Inputs(ctx context.Context, l2SafeHead eth.L2BlockRef) (*eth.PayloadAttributes, error) {
	if len(pq.l1Origins) == 0 {
		return nil, io.EOF
	}
	if len(pq.batches) == 0 {
		return nil, io.EOF
	}
	batch := pq.batches[0]
	// TODO: Don't slice until later?
	pq.batches = pq.batches[1:]

	seqNumber := l2SafeHead.SequenceNumber + 1
	l1Origin := pq.l1Origins[0]

	// Check if we need to advance an epoch
	if l1Origin.Number != uint64(batch.Batch.Epoch) {
		seqNumber = 0
		// Not enough saved origins to advance an epoch here.
		if len(pq.l1Origins) < 2 {
			return nil, io.EOF
		}
		l1Origin = pq.l1Origins[1]
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	l1Info, _, receipts, err := pq.dl.Fetch(fetchCtx, l1Origin.Hash)
	if err != nil {
		pq.log.Error("failed to fetch L1 block info", "l1Origin", l1Origin, "err", err)
		return nil, err
	}

	var deposits []hexutil.Bytes
	// Fill in deposits if we have advanced an epoch
	if pq.l1Origins[0].Number != uint64(batch.Batch.Epoch) {
		fetchCtx, cancel = context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		var errs []error
		deposits, errs = DeriveDeposits(receipts, pq.config.DepositContractAddress)
		for _, err := range errs {
			pq.log.Error("Failed to derive a deposit", "l1OriginHash", l1Origin.Hash, "err", err)
		}
		if len(errs) != 0 {
			// TODO: Multierror here
			return nil, fmt.Errorf("failed to derive some deposits: %v", errs)
		}
	}

	var txns []eth.Data
	l1InfoTx, err := L1InfoDepositBytes(seqNumber, l1Info)
	if err != nil {
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
		SuggestedFeeRecipient: pq.config.FeeRecipientAddress,
		Transactions:          txns,
		// we are verifying, not sequencing, we've got all transactions and do not pull from the tx-pool
		// (that would make the block derivation non-deterministic)
		NoTxPool: true,
	}

	return attrs, nil
}
