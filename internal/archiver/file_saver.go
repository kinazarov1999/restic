package archiver

import (
	"context"
	"fmt"
	"github.com/restic/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"
)

// SaveBlobFn saves a blob to a repo.
type SaveBlobFn func(context.Context, restic.BlobType, *Buffer, string, func(res SaveBlobResponse))

// FileSaver concurrently saves incoming files to the repo.
type FileSaver struct {
	saveFilePool *BufferPool
	saveBlob     SaveBlobFn

	pol chunker.Pol

	ch chan<- saveFileJob

	CompleteBlob func(bytes uint64)

	NodeFromFileInfo func(snPath, filename string, fi os.FileInfo, ignoreXattrListError bool) (*restic.Node, error)
}

// NewFileSaver returns a new file saver. A worker pool with fileWorkers is
// started, it is stopped when ctx is cancelled.
func NewFileSaver(ctx context.Context, wg *errgroup.Group, save SaveBlobFn, pol chunker.Pol, fileWorkers, blobWorkers uint) *FileSaver {
	ch := make(chan saveFileJob)

	debug.Log("new file saver with %v file workers and %v blob workers", fileWorkers, blobWorkers)

	poolSize := fileWorkers + blobWorkers

	s := &FileSaver{
		saveBlob:     save,
		saveFilePool: NewBufferPool(int(poolSize), chunker.MaxSize),
		pol:          pol,
		ch:           ch,

		CompleteBlob: func(uint64) {},
	}

	for i := uint(0); i < fileWorkers; i++ {
		wg.Go(func() error {
			s.worker(ctx, ch)
			return nil
		})
	}

	return s
}

func (s *FileSaver) TriggerShutdown() {
	close(s.ch)
}

func getBlockDeviceSize(f fs.File) (int64, error) {
	// Prepare to call the ioctl operation.
	var size int64
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))

	if errno != 0 {
		fileInfo, _ := f.Stat()
		// Get and print the size from FileInfo
		return fileInfo.Size(), nil
	}
	if errno != 0 {
		return 0, errno
	}

	return size, nil
}

// CompleteFunc is called when the file has been saved.
type CompleteFunc func(*restic.Node, ItemStats)

// Save stores the file f and returns the data once it has been completed. The
// file is closed by Save. completeReading is only called if the file was read
// successfully. complete is always called. If completeReading is called, then
// this will always happen before calling complete.
func (s *FileSaver) Save(ctx context.Context, snPath string, target string, file fs.File, fi os.FileInfo, start func(), completeReading func(), complete CompleteFunc, parallelize bool, workersCount int, blockSizeMB int) FutureNode {
	fn, ch := newFutureNode()
	debug.Log("<<<<<<<Entered Save routine")

	job := saveFileJob{
		snPath: snPath,
		target: target,
		file:   file,
		fi:     fi,
		ch:     ch,

		parallelize:  parallelize,
		workersCount: workersCount,
		blockSizeMB:  blockSizeMB,

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
	fi     os.FileInfo
	ch     chan<- futureNodeResult

	parallelize  bool
	workersCount int
	blockSizeMB  int

	start           func()
	completeReading func()
	complete        CompleteFunc
}

// saveFile stores the file f in the repo, then closes it.
func (s *FileSaver) saveFile(ctx context.Context, chnker *chunker.Chunker, snPath string, target string, f fs.File, fi os.FileInfo, start func(), finishReading func(), finish func(res futureNodeResult)) {
	start()

	fnr := futureNodeResult{
		snPath: snPath,
		target: target,
	}
	var lock sync.Mutex
	remaining := 0
	isCompleted := false

	completeBlob := func() {
		lock.Lock()
		defer lock.Unlock()

		remaining--
		if remaining == 0 {
			debug.Log("Remaining is zero.")
		}
		if remaining == 0 && fnr.err == nil {
			debug.Log("Running the final completeBlob ...")
			if isCompleted {
				panic("completed twice")
			}
			for _, id := range fnr.node.Content {
				if id.IsNull() {
					panic("completed file with null ID")
				}
			}
			isCompleted = true
			finish(fnr)
			debug.Log("Done with the final completeBlob")
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

	node, err := s.NodeFromFileInfo(snPath, target, fi, false)
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	debug.Log("<<<<<After NodeFromFileInfo call")
	//if node.Type != "file" {
	//	_ = f.Close()
	//	completeError(errors.Errorf("node type %q is wrong", node.Type))
	//	return
	//}

	// reuse the chunker
	chnker.Reset(f, s.pol)
	debug.Log("<<<<<After chunker reset")

	node.Content = []restic.ID{}
	node.Size = 0
	var idx int
	for {
		buf := s.saveFilePool.Get()
		debug.Log("<<<<<Before chunker.Next")
		chunk, err := chnker.Next(buf.Data)
		debug.Log("<<<<<After chunker.Next")
		if err == io.EOF {
			buf.Release()
			break
		}

		buf.Data = chunk.Data
		node.Size += uint64(chunk.Length)

		if err != nil {
			_ = f.Close()
			completeError(err)
			return
		}
		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = f.Close()
			completeError(ctx.Err())
			return
		}

		// add a place to store the saveBlob result
		pos := idx

		lock.Lock()
		node.Content = append(node.Content, restic.ID{})
		lock.Unlock()

		s.saveBlob(ctx, restic.DataBlob, buf, target, func(sbr SaveBlobResponse) {
			lock.Lock()
			if !sbr.known {
				fnr.stats.DataBlobs++
				fnr.stats.DataSize += uint64(sbr.length)
				fnr.stats.DataSizeInRepo += uint64(sbr.sizeInRepo)
			}

			node.Content[pos] = sbr.id
			lock.Unlock()

			completeBlob()
		})
		idx++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = f.Close()
			completeError(ctx.Err())
			return
		}

		s.CompleteBlob(uint64(len(chunk.Data)))
	}

	err = f.Close()
	if err != nil {
		completeError(err)
		return
	}

	fnr.node = node
	lock.Lock()
	// require one additional completeFuture() call to ensure that the future only completes
	// after reaching the end of this method
	remaining += idx + 1
	debug.Log("<<<<<idx: %d", idx)
	lock.Unlock()
	finishReading()
	completeBlob()
	debug.Log("<<<<<<Finished file saving.")
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	debug.Log("<<<<<<Number of GCs: %v\n", stats.NumGC)
	debug.Log("<<<<<<Total GC pause time: %v ms\n", stats.PauseTotalNs/1e6)
}

// saveFile stores the file f in the repo, then closes it.
func (s *FileSaver) saveFileParallel(ctx context.Context, snPath string, target string, f fs.File, fi os.FileInfo, start func(), finishReading func(), finish func(res futureNodeResult), workersCount int, blockSizeMB int) {
	start()

	fnr := futureNodeResult{
		snPath: snPath,
		target: target,
	}
	var lock sync.Mutex
	remaining := 0
	isCompleted := false

	completeBlob := func() {
		lock.Lock()
		defer lock.Unlock()

		remaining--
		debug.Log("<<<<<<Remaining: %d", remaining)
		if remaining == 0 && fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			badId := false
			for i, id := range fnr.node.Content {
				if id.IsNull() {
					debug.Log("<<<<<<Null at %d", i)
					badId = true
					//panic("completed file with null ID")
				}
			}
			if badId {
				panic("completed file with null ID")
			}
			isCompleted = true
			//finish(fnr)
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

	node, err := s.NodeFromFileInfo(snPath, target, fi, false)
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	debug.Log("<<<<<After NodeFromFileInfo call")
	//if node.Type != "file" {
	//	_ = f.Close()
	//	completeError(errors.Errorf("node type %q is wrong", node.Type))
	//	return
	//}

	// reuse the chunker
	chnker := chunker.New(nil, s.pol)
	chnker.Reset(f, s.pol)
	debug.Log("<<<<<After chunker reset")

	jobs := make(chan processBlobJob, 10)
	results := make(chan processBlobResult, 10)

	if blockSizeMB == 0 {
		blockSizeMB = 16
	}
	blockSize := (int64(1) << 20) * int64(blockSizeMB)

	sizeBytes, _ := getBlockDeviceSize(f)
	offsetStart := int64(0)
	offsetEnd := offsetStart + blockSize
	debug.Log("<<<<<<<file size: %d bytes ...", sizeBytes)

	go func() {
		defer close(jobs)
		id := int64(0)
		for offsetStart < sizeBytes {
			jobs <- processBlobJob{id, offsetStart, offsetEnd}
			offsetStart += blockSize
			offsetEnd += blockSize
			id++
		}
	}()

	wg, _ := errgroup.WithContext(context.Background())

	if workersCount == 0 {
		workersCount = 1
	}

	go func() {
		defer close(results)
		for i := 0; i < workersCount; i++ {
			wg.Go(func() error {
				s.processBlobWorker(jobs, results, ctx, target, f, &lock, &fnr, completeBlob)
				return nil
			})
		}
		wg.Wait()
	}()

	node.Size = 0
	totalChunks := 0
	contentMap := make(map[int64][]restic.ID)

	for result := range results {
		if result.err != nil {
			_ = f.Close()
			completeError(err)
			return
		}
		node.Size += result.size
		totalChunks += result.totalChunks
		contentMap[result.id] = result.content
	}

	fnr.node = node
	lock.Lock()
	// require one additional completeFuture() call to ensure that the future only completes
	// after reaching the end of this method
	remaining += totalChunks + 1
	lock.Unlock()
	finishReading()
	debug.Log("<<<<<<<Done saving file.")
	completeBlob()
	debug.Log("<<<<<<<Completed the last blob.")

	// wait for routines to stop so that contentMap is populated
	// TODO: come up with something better
	for !isCompleted {
		time.Sleep(time.Millisecond)
	}

	contentKeys := make([]int, 0, len(contentMap))
	for key := range contentMap {
		contentKeys = append(contentKeys, int(key))
	}
	sort.Ints(contentKeys)

	node.Content = []restic.ID{}

	for _, key := range contentKeys {
		node.Content = append(node.Content, contentMap[int64(key)]...)
	}
	finish(fnr)
	debug.Log("<<<<<<<<<<Length of content: %d", len(node.Content))
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	debug.Log("<<<<<<Number of GCs: %v\n", stats.NumGC)
	debug.Log("<<<<<<Total GC pause time: %v ms\n", stats.PauseTotalNs/1e6)

}

type processBlobJob struct {
	id          int64
	offsetStart int64
	offsetEnd   int64
}

type processBlobResult struct {
	id          int64
	size        uint64
	content     []restic.ID
	totalChunks int
	err         error
}

func (s *FileSaver) processBlobWorker(
	jobs <-chan processBlobJob,
	results chan<- processBlobResult,
	ctx context.Context,
	target string,
	f fs.File,
	lock *sync.Mutex,
	fnr *futureNodeResult,
	completeBlob func(),
) {
	chnker := chunker.New(nil, s.pol)
	debug.Log("<<<<<Started processBlobWorker.")

	for job := range jobs {
		debug.Log("<<<<<Got a job at %d", job.offsetStart)
		res := s.processBlob(ctx, target, f, lock, fnr, completeBlob, chnker, job.offsetStart, job.offsetEnd, job.id)
		debug.Log("<<<<<Processed a job at %d", job.offsetStart)
		results <- res
		debug.Log("<<<<<Successfully put result of the job at %d into the channel.", job.offsetStart)
	}
}

func (s *FileSaver) processBlob(
	ctx context.Context,
	target string,
	f fs.File,
	lock *sync.Mutex,
	fnr *futureNodeResult,
	completeBlob func(),
	chnker *chunker.Chunker,
	offsetStart int64,
	offsetEnd int64,
	id int64,
) processBlobResult {

	var idx int
	content := make([]restic.ID, 0)
	size := uint64(0)
	pf := fs.NewPartialFile(f, offsetStart, offsetEnd)
	chnker.Reset(pf, s.pol)

	for {
		buf := s.saveFilePool.Get()
		debug.Log("<<<<<Before chunker.Next")
		chunk, err := chnker.Next(buf.Data)
		debug.Log("<<<<<After chunker.Next")
		if err == io.EOF {
			debug.Log("<<<<<EOF")
			buf.Release()
			break
		}

		buf.Data = chunk.Data
		size += uint64(chunk.Length)

		if err != nil {
			return processBlobResult{
				err: err,
			}
		}
		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			return processBlobResult{
				err: ctx.Err(),
			}
		}

		// add a place to store the saveBlob result
		pos := idx

		content = append(content, restic.ID{})

		s.saveBlob(ctx, restic.DataBlob, buf, target, func(sbr SaveBlobResponse) {
			lock.Lock()
			if !sbr.known {
				fnr.stats.DataBlobs++
				fnr.stats.DataSize += uint64(sbr.length)
				fnr.stats.DataSizeInRepo += uint64(sbr.sizeInRepo)
			}

			content[pos] = sbr.id
			lock.Unlock()

			completeBlob()
		})
		idx++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			return processBlobResult{
				err: ctx.Err(),
			}
		}

		s.CompleteBlob(uint64(len(chunk.Data)))
	}
	return processBlobResult{id, size, content, idx, nil}
}

func (s *FileSaver) worker(ctx context.Context, jobs <-chan saveFileJob) {
	// a worker has one chunker which is reused for each file (because it contains a rather large buffer)
	//chnker := chunker.New(nil, s.pol)

	for {
		var job saveFileJob
		var ok bool
		select {
		case <-ctx.Done():
			debug.Log("<<<<<<<<<<Context is done!")
			return
		case job, ok = <-jobs:
			if !ok {
				debug.Log("<<<<<<<<<<Job is not ok!")
				return
			}
		}
		debug.Log("<<<<<<Got a job to do!")

		if !job.parallelize {
			chnker := chunker.New(nil, s.pol)
			s.saveFile(ctx, chnker, job.snPath, job.target, job.file, job.fi, job.start, func() {
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
		} else {
			s.saveFileParallel(ctx, job.snPath, job.target, job.file, job.fi, job.start, func() {
				if job.completeReading != nil {
					job.completeReading()
				}
			}, func(res futureNodeResult) {
				if job.complete != nil {
					job.complete(res.node, res.stats)
				}
				job.ch <- res
				close(job.ch)
			}, job.workersCount, job.blockSizeMB)

		}
		debug.Log("<<<<<<Finished the job.")
	}
}
