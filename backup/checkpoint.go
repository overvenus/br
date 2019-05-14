package backup

import (
	"bytes"
	"context"
	"errors"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// DoCheckpoint returns a checkpoint.
func (backer *Backer) DoCheckpoint() ([]*RangeMeta, error) {
	physical, logical, err := backer.store.pdClient.GetTS(backer.ctx)

	checkpoint := Timestamp{
		Physical: physical - MaxTxnTimeUse,
		Logical:  logical,
	}

	handler := func(ctx context.Context, r kv.KeyRange) (int, error) {
		return backer.readIndexForRange(ctx, r.StartKey, r.EndKey)
	}

	// TODO: update after https://github.com/pingcap/tidb/pull/10379 is merged
	runner := tikv.NewRangeTaskRunner("read-index-runner", backer.store, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))

	return []*RangeMeta{}, nil
}

func (backer *Backer) readIndexForRange(
	ctx context.Context,
	startKey []byte,
	endKey []byte,
) (int, error) {
	// TODO: update github.com/pingcap/tidb/store/tikv/tikvrpc to support ReadIndex
	req := &tikvrpc.Request{
		Type:      tikvrpc.CmdReadIndex,
		ReadIndex: &kvrpcpb.ReadIndexRequest{},
	}

	regions := 0
	key := startKey

	for {
		select {
		case <-ctx.Done():
			return regions, errors.New("backup check point canceled")
		default:
		}

		bo := tikv.NewBackoffer(ctx, tikv.ReadIndexMaxBackoff)
		loc, err := backer.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return regions, errors.Trace(err)
		}
		resp, err := backer.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return regions, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return regions, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return regions, errors.Trace(err)
			}
			continue
		}

		readIndexResp := resp.ReadIndex
		if readIndexResp == nil {
			return regions, errors.Trace(tikv.ErrBodyMissing)
		}

		// seems useless?
		// index := readIndexResp.GetReadIndex()

		regions++
		key = loc.EndKey

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
	}
	return regions, nil
}
