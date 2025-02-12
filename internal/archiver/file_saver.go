package archiver

import (
	"context"
	"fmt"
	"github.com/restic/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"io"
	"sync"
	"unsafe"
)

// saveBlobFn saves a blob to a repo.
type saveBlobFn func(context.Context, restic.BlobType, *buffer, string, func(res saveBlobResponse))

// fileSaver concurrently saves incoming files to the repo.
type fileSaver struct {
	saveFilePool *bufferPool
	saveBlob     saveBlobFn

	pol chunker.Pol

	ch chan<- saveFileJob

	CompleteBlob func(bytes uint64)

	NodeFromFileInfo func(snPath, filename string, meta ToNoder, ignoreXattrListError bool) (*restic.Node, error)

	perFileWorkers uint
	blockSizeMB    uint
}

// newFileSaver returns a new file saver. A worker pool with fileWorkers is
// started, it is stopped when ctx is cancelled.
func newFileSaver(ctx context.Context, wg *errgroup.Group, save saveBlobFn, pol chunker.Pol, fileWorkers, blobWorkers, blockSizeMB uint) *fileSaver {
	ch := make(chan saveFileJob)

	debug.Log("new file saver with %v file workers and %v blob workers", fileWorkers, blobWorkers)

	poolSize := fileWorkers + blobWorkers

	var perFileWorkers uint
	if blockSizeMB == 0 {
		perFileWorkers = 1
	} else {
		perFileWorkers = fileWorkers
		fileWorkers = 1
	}

	s := &fileSaver{
		saveBlob:     save,
		saveFilePool: newBufferPool(int(poolSize), chunker.MaxSize),
		pol:          pol,
		ch:           ch,

		CompleteBlob: func(uint64) {},

		perFileWorkers: perFileWorkers,
		blockSizeMB:    blockSizeMB,
	}

	for i := uint(0); i < fileWorkers; i++ {
		wg.Go(func() error {
			s.worker(ctx, ch)
			return nil
		})
	}

	return s
}

func (s *fileSaver) TriggerShutdown() {
	close(s.ch)
}

// fileCompleteFunc is called when the file has been saved.
type fileCompleteFunc func(*restic.Node, ItemStats)

// Save stores the file f and returns the data once it has been completed. The
// file is closed by Save. completeReading is only called if the file was read
// successfully. complete is always called. If completeReading is called, then
// this will always happen before calling complete.
func (s *fileSaver) Save(ctx context.Context, snPath string, target string, file fs.File, start func(), completeReading func(), complete fileCompleteFunc) futureNode {
	fn, ch := newFutureNode()

	job := saveFileJob{
		snPath: snPath,
		target: target,
		file:   file,
		ch:     ch,

		start:           start,
		completeReading: completeReading,
		complete:        complete,
	}

	select {
	case s.ch <- job:
	case <-ctx.Done():
		debug.Log("not sending job, context is cancelled: %v", ctx.Err())
		_ = file.Close()
		close(ch)
	}

	return fn
}

type saveFileJob struct {
	snPath string
	target string
	file   fs.File
	ch     chan<- futureNodeResult

	start           func()
	completeReading func()
	complete        fileCompleteFunc
}

// saveFile stores the file f in the repo, then closes it.
func (s *fileSaver) saveFile(ctx context.Context, snPath string, target string, f fs.File, start func(), finishReading func(), finish func(res futureNodeResult)) {
	start()

	fnr := futureNodeResult{
		snPath: snPath,
		target: target,
	}
	var lock sync.Mutex
	remaining := 0
	isCompleted := false
	contentMap := make(map[int64][]restic.ID)

	completeBlob := func(node *restic.Node) {
		lock.Lock()
		defer lock.Unlock()

		remaining--
		if remaining == 0 && fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			for _, id := range fnr.node.Content {
				if id.IsNull() {
					panic("completed file with null ID")
				}
			}
			isCompleted = true

			node.Content = []restic.ID{}
			curIdx := 0
			for {
				contentValue, ok := contentMap[int64(curIdx)]
				if !ok {
					break
				}
				node.Content = append(node.Content, contentValue...)
				curIdx++
			}

			finish(fnr)
		}
	}

	completeError := func(err error) {
		lock.Lock()
		defer lock.Unlock()

		if fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			isCompleted = true
			fnr.err = fmt.Errorf("failed to save %v: %w", target, err)
			fnr.node = nil
			fnr.stats = ItemStats{}
			finish(fnr)
		}
	}

	debug.Log("%v", snPath)

	node, err := s.NodeFromFileInfo(snPath, target, f, false)
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	// reset node size, as we're going to calculate it
	node.Size = 0
	fnr.node = node
	debug.Log("Node size: %d", fnr.node.Size)

	if (node.Type != restic.NodeTypeFile) && (node.Type != restic.NodeTypeDev) {
		_ = f.Close()
		completeError(errors.Errorf("node type %q is wrong", node.Type))
		return
	}

	// reuse the chunker
	chnker := chunker.New(nil, s.pol)
	chnker.Reset(f, s.pol)

	jobs := make(chan processBlobJob)
	results := make(chan int)

	blockSize := (int64(1) << 20) * int64(s.blockSizeMB)

	var sizeBytes int64
	if node.Type != restic.NodeTypeDev {
		stat, err := f.Stat()
		if err != nil {
			_ = f.Close()
			completeError(err)
			return
		}
		sizeBytes = stat.Size
	} else {
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&sizeBytes)))
		if errno != 0 {
			_ = f.Close()
			completeError(err)
			return
		}
	}

	offsetStart := int64(0)

	go func() {
		defer close(jobs)
		id := int64(0)
		if blockSize == 0 {
			jobs <- processBlobJob{0, 0, 0}
		} else {
			for offsetStart < sizeBytes {
				jobs <- processBlobJob{id, offsetStart, blockSize}
				offsetStart += blockSize
				id++
			}
		}
	}()

	wg, innerCtx := errgroup.WithContext(context.Background())

	go func() {
		defer close(results)
		for i := uint(0); i < s.perFileWorkers; i++ {
			wg.Go(func() error {
				return s.processBlobWorker(jobs, results, ctx, innerCtx, target, f, &lock, &fnr, completeBlob, contentMap)
			})
		}
		wg.Wait()
	}()

	totalChunks := 0

	for result := range results {
		totalChunks += result
	}

	// there should be a better way to find out whether there was an error
	err = wg.Wait()
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	err = f.Close()
	if err != nil {
		completeError(err)
		return
	}

	lock.Lock()
	// require one additional completeFuture() call to ensure that the future only completes
	// after reaching the end of this method
	remaining += totalChunks + 1
	lock.Unlock()
	finishReading()
	completeBlob(fnr.node)
}

type processBlobJob struct {
	id          int64
	offsetStart int64
	blockSize   int64
}

func (s *fileSaver) processBlobWorker(
	jobs <-chan processBlobJob,
	results chan<- int,
	ctx context.Context,
	innerCtx context.Context,
	target string,
	f fs.File,
	lock *sync.Mutex,
	fnr *futureNodeResult,
	completeBlob func(node *restic.Node),
	contentMap map[int64][]restic.ID,
) error {
	chnker := chunker.New(nil, s.pol)

	for {
		var job processBlobJob
		var ok bool
		select {
		case <-innerCtx.Done():
			return nil
		case job, ok = <-jobs:
			if !ok {
				return nil
			}
		}
		var reader io.Reader
		if job.blockSize == 0 {
			// '0' indicates that we do not cut the file in parts
			reader = f
		} else {
			reader = io.NewSectionReader(f, job.offsetStart, job.blockSize)
		}
		res, err := s.processBlobs(ctx, target, reader, lock, fnr, completeBlob, chnker, job.id, contentMap)
		if err != nil {
			return err
		}
		results <- res
	}
}

func (s *fileSaver) processBlobs(
	ctx context.Context,
	target string,
	f io.Reader,
	lock *sync.Mutex,
	fnr *futureNodeResult,
	completeBlob func(node *restic.Node),
	chnker *chunker.Chunker,
	id int64,
	contentMap map[int64][]restic.ID,
) (int, error) {

	var chunksCount int
	size := uint64(0)
	chnker.Reset(f, s.pol)
	lock.Lock()
	contentMap[id] = make([]restic.ID, 0)
	lock.Unlock()

	for {
		buf := s.saveFilePool.Get()
		chunk, err := chnker.Next(buf.Data)
		if err == io.EOF {
			buf.Release()
			break
		}

		if err != nil {
			return 0, err
		}

		buf.Data = chunk.Data

		size = uint64(chunk.Length)

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		// add a place to store the saveBlob result
		// redefinition of pos on each iteration is needed as we pass it to saveBlob cb
		pos := chunksCount

		lock.Lock()
		contentMap[id] = append(contentMap[id], restic.ID{})
		fnr.node.Size += size
		lock.Unlock()

		s.saveBlob(ctx, restic.DataBlob, buf, target, func(sbr saveBlobResponse) {
			lock.Lock()
			if !sbr.known {
				fnr.stats.DataBlobs++
				fnr.stats.DataSize += uint64(sbr.length)
				fnr.stats.DataSizeInRepo += uint64(sbr.sizeInRepo)
			}

			contentMap[id][pos] = sbr.id
			lock.Unlock()

			completeBlob(fnr.node)
		})
		chunksCount++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		s.CompleteBlob(uint64(len(chunk.Data)))
	}
	return chunksCount, nil
}

func (s *fileSaver) worker(ctx context.Context, jobs <-chan saveFileJob) {
	// a worker has one chunker which is reused for each file (because it contains a rather large buffer)
	//chnker := chunker.New(nil, s.pol)

	for {
		var job saveFileJob
		var ok bool
		select {
		case <-ctx.Done():
			return
		case job, ok = <-jobs:
			if !ok {
				return
			}
		}

		s.saveFile(ctx, job.snPath, job.target, job.file, job.start, func() {
			if job.completeReading != nil {
				job.completeReading()
			}
		}, func(res futureNodeResult) {
			if job.complete != nil {
				job.complete(res.node, res.stats)
			}
			job.ch <- res
			close(job.ch)
		})
	}
}
