package dump

import (
	"context"
	"github.com/restic/restic/internal/repository"
	"io"
	"math/rand"
	"path"
	"sync"

	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/walker"
	"golang.org/x/sync/errgroup"
)

// A Dumper writes trees and files from a repository to a Writer
// in an archive format.
type Dumper struct {
	cache  *bloblru.Cache
	format string
	repo   restic.Loader
	w      io.Writer
}

type ParallelDumper struct {
	cache           *bloblru.Cache
	format          string
	repo            restic.Loader
	w               io.WriterAt
	shuffle         bool
	roundRobin      bool
	separateLoaders bool
	blockSizeMiB    uint
}

type NodeWriter interface {
	WriteNode(ctx context.Context, node *data.Node) error
}

type TreeDumper interface {
	DumpTree(ctx context.Context, tree *data.Tree, rootPath string) error
}

func New(format string, repo restic.Loader, w io.Writer) *Dumper {
	return &Dumper{
		cache:  bloblru.New(64 << 20),
		format: format,
		repo:   repo,
		w:      w,
	}
}

func NewParallelDumper(dumper *Dumper, w io.WriterAt, shuffle, roundRobin, separateLoaders bool, blockSizeMiB uint) *ParallelDumper {
	return &ParallelDumper{
		cache:           dumper.cache,
		format:          dumper.format,
		repo:            dumper.repo,
		w:               w,
		shuffle:         shuffle,
		roundRobin:      roundRobin,
		separateLoaders: separateLoaders,
		blockSizeMiB:    blockSizeMiB,
	}
}

func (d *Dumper) DumpTree(ctx context.Context, tree *data.Tree, rootPath string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ch is buffered to deal with variable download/write speeds.
	ch := make(chan *data.Node, 10)
	go sendTrees(ctx, d.repo, tree, rootPath, ch)

	switch d.format {
	case "tar":
		return d.dumpTar(ctx, ch)
	case "zip":
		return d.dumpZip(ctx, ch)
	default:
		panic("unknown dump format")
	}
}

func sendTrees(ctx context.Context, repo restic.BlobLoader, tree *data.Tree, rootPath string, ch chan *data.Node) {
	defer close(ch)

	for _, root := range tree.Nodes {
		root.Path = path.Join(rootPath, root.Name)
		if sendNodes(ctx, repo, root, ch) != nil {
			break
		}
	}
}

func sendNodes(ctx context.Context, repo restic.BlobLoader, root *data.Node, ch chan *data.Node) error {
	select {
	case ch <- root:
	case <-ctx.Done():
		return ctx.Err()
	}

	// If this is no directory we are finished
	if root.Type != data.NodeTypeDir {
		return nil
	}

	err := walker.Walk(ctx, repo, *root.Subtree, walker.WalkVisitor{ProcessNode: func(_ restic.ID, nodepath string, node *data.Node, err error) error {
		if err != nil {
			return err
		}
		if node == nil {
			return nil
		}

		node.Path = path.Join(root.Path, nodepath)

		if node.Type != data.NodeTypeFile && node.Type != data.NodeTypeDir && node.Type != data.NodeTypeSymlink {
			return nil
		}

		select {
		case ch <- node:
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	}})

	return err
}

// WriteNode writes a file node's contents directly to d's Writer,
// without caring about d's format.
func (d *Dumper) WriteNode(ctx context.Context, node *data.Node) error {
	return d.writeNode(ctx, d.w, node)
}

func (d *Dumper) writeNode(ctx context.Context, w io.Writer, node *data.Node) error {
	wg, ctx := errgroup.WithContext(ctx)
	limit := int(d.repo.Connections())
	wg.SetLimit(1 + limit) // +1 for the writer.
	blobs := make(chan (<-chan []byte), limit)

	// Writer.
	wg.Go(func() error {
		for ch := range blobs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case blob := <-ch:
				if _, err := w.Write(blob); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// Start short-lived goroutines to load blobs.
loop:
	for _, id := range node.Content {
		// This needs to be buffered, so that loaders can quit
		// without waiting for the writer.
		ch := make(chan []byte, 1)

		wg.Go(func() error {
			blob, err := d.cache.GetOrCompute(id, func() ([]byte, error) {
				return d.repo.LoadBlob(ctx, restic.DataBlob, id, nil)
			})

			if err == nil {
				ch <- blob
			}
			return err
		})

		select {
		case blobs <- ch:
		case <-ctx.Done():
			break loop
		}
	}

	close(blobs)
	return wg.Wait()
}

type Blob struct {
	data   []byte
	offset int64
}

func (p *ParallelDumper) WriteNode(ctx context.Context, node *data.Node) error {
	return p.writeNode(ctx, p.w, node)
}

type BlobTask struct {
	id     restic.ID
	offset int64
}

func (p *ParallelDumper) writeNode(ctx context.Context, w io.WriterAt, node *data.Node) error {
	if p.blockSizeMiB != 0 {
		if p.separateLoaders {
			return p.writeNodeBlocksSeparateLoader(ctx, w, node)
		}
		return p.writeNodeBlocks(ctx, w, node)
	}
	if p.separateLoaders {
		return p.writeNodeSeparateLoader(ctx, w, node)
	}
	wg, ctx := errgroup.WithContext(ctx)
	limit := int(p.repo.Connections())
	wg.SetLimit(limit)

	// Create slice to hold tasks
	tasks := make([]BlobTask, 0, len(node.Content))

	var currentOffset int64 = 0
	for _, id := range node.Content {
		size, _ := p.repo.LookupBlobSize(restic.DataBlob, id)
		tasks = append(tasks, BlobTask{
			id:     id,
			offset: currentOffset,
		})
		currentOffset += int64(size)
	}

	if p.shuffle {
		// Shuffle the tasks
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
	}

	if p.roundRobin {
		N := len(tasks)
		if limit > 1 {
			reordered := make([]BlobTask, 0, N)
			stride := (N + limit - 1) / limit

			for i := 0; i < stride; i++ {
				for j := 0; j < limit; j++ {
					idx := j*stride + i
					if idx < N {
						reordered = append(reordered, tasks[idx])
					}
				}
			}
			tasks = reordered
		}
	}

	// Create channel and send shuffled tasks
	taskChan := make(chan BlobTask, len(tasks))
	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	for i := 0; i < limit; i++ {
		wg.Go(func() error {
			for task := range taskChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if task.id == repository.ZeroChunk() {
						continue
					}
					blob, err := p.cache.GetOrCompute(task.id, func() ([]byte, error) {
						return p.repo.LoadBlob(ctx, restic.DataBlob, task.id, nil)
					})
					if err != nil {
						return err
					}
					if _, err := w.WriteAt(blob, task.offset); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	return wg.Wait()
}

type BlockBlobTask struct {
	blobTasks []BlobTask
}

func (p *ParallelDumper) writeNodeBlocks(ctx context.Context, w io.WriterAt, node *data.Node) error {
	wg, ctx := errgroup.WithContext(ctx)
	limit := int(p.repo.Connections())
	wg.SetLimit(limit)

	blockSize := p.blockSizeMiB << 20

	taskChan := make(chan BlockBlobTask, len(node.Content))
	var currentOffset int64 = 0
	var currentSize uint = 0
	var blockTask BlockBlobTask
	for _, id := range node.Content {
		size, _ := p.repo.LookupBlobSize(restic.DataBlob, id)
		currentSize += size
		blockTask.blobTasks = append(blockTask.blobTasks, BlobTask{
			id:     id,
			offset: currentOffset,
		})
		if currentSize >= blockSize {
			taskChan <- blockTask
			blockTask = BlockBlobTask{}
			currentSize = 0
		}
		currentOffset += int64(size)
	}

	if len(blockTask.blobTasks) != 0 {
		taskChan <- blockTask
	}

	close(taskChan)

	for i := 0; i < limit; i++ {
		wg.Go(func() error {
			for blockTask := range taskChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					for _, task := range blockTask.blobTasks {
						blob, err := p.cache.GetOrCompute(task.id, func() ([]byte, error) {
							return p.repo.LoadBlob(ctx, restic.DataBlob, task.id, nil)
						})
						if err != nil {
							return err
						}

						if _, err := w.WriteAt(blob, task.offset); err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
	}

	return wg.Wait()
}

func (p *ParallelDumper) writeNodeSeparateLoader(ctx context.Context, w io.WriterAt, node *data.Node) error {
	if p.blockSizeMiB != 0 {
		return p.writeNodeBlocksSeparateLoader(ctx, w, node)
	}

	eg, ctx := errgroup.WithContext(ctx)
	limit := int(p.repo.Connections())
	eg.SetLimit(limit * 2) // Both loaders and writers

	// Create slice to hold tasks
	tasks := make([]BlobTask, 0, len(node.Content))

	var currentOffset int64 = 0
	for _, id := range node.Content {
		size, _ := p.repo.LookupBlobSize(restic.DataBlob, id)
		tasks = append(tasks, BlobTask{
			id:     id,
			offset: currentOffset,
		})
		currentOffset += int64(size)
	}

	if p.shuffle {
		// Shuffle the tasks
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
	}

	if p.roundRobin {
		N := len(tasks)
		if limit > 1 {
			reordered := make([]BlobTask, 0, N)
			stride := (N + limit - 1) / limit

			for i := 0; i < stride; i++ {
				for j := 0; j < limit; j++ {
					idx := j*stride + i
					if idx < N {
						reordered = append(reordered, tasks[idx])
					}
				}
			}
			tasks = reordered
		}
	}

	// Create channels for pipeline
	taskChan := make(chan BlobTask, len(tasks))
	loadedBlobChan := make(chan struct {
		data   []byte
		offset int64
	}, limit*2)

	// Send tasks to channel
	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	// Track number of loader goroutines that are done
	var loaderDone sync.WaitGroup
	loaderDone.Add(limit)

	// Start loader workers
	for i := 0; i < limit; i++ {
		eg.Go(func() error {
			defer loaderDone.Done()
			for task := range taskChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					blob, err := p.cache.GetOrCompute(task.id, func() ([]byte, error) {
						return p.repo.LoadBlob(ctx, restic.DataBlob, task.id, nil)
					})
					if err != nil {
						return err
					}

					select {
					case loadedBlobChan <- struct {
						data   []byte
						offset int64
					}{data: blob, offset: task.offset}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return nil
		})
	}

	// Close loadedBlobChan when all loaders are done
	go func() {
		loaderDone.Wait()
		close(loadedBlobChan)
	}()

	// Start writer workers
	for i := 0; i < limit; i++ {
		eg.Go(func() error {
			for loadedBlob := range loadedBlobChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if _, err := w.WriteAt(loadedBlob.data, loadedBlob.offset); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

func (p *ParallelDumper) writeNodeBlocksSeparateLoader(ctx context.Context, w io.WriterAt, node *data.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	limit := int(p.repo.Connections())
	eg.SetLimit(limit * 2) // Both loaders and writers

	blockSize := p.blockSizeMiB << 20

	// Channel for block tasks (to loaders)
	blockTaskChan := make(chan BlockBlobTask, len(node.Content))

	// Channel for individual blob tasks ready to write
	type LoadedBlob struct {
		data   []byte
		offset int64
	}
	loadedBlobChan := make(chan LoadedBlob, limit*4)

	var currentOffset int64 = 0
	var currentSize uint = 0
	var blockTask BlockBlobTask
	for _, id := range node.Content {
		size, _ := p.repo.LookupBlobSize(restic.DataBlob, id)
		currentSize += size
		blockTask.blobTasks = append(blockTask.blobTasks, BlobTask{
			id:     id,
			offset: currentOffset,
		})
		if currentSize >= blockSize {
			blockTaskChan <- blockTask
			blockTask = BlockBlobTask{}
			currentSize = 0
		}
		currentOffset += int64(size)
	}

	if len(blockTask.blobTasks) != 0 {
		blockTaskChan <- blockTask
	}
	close(blockTaskChan)

	// Track number of loader goroutines that are done
	var loaderDone sync.WaitGroup
	loaderDone.Add(limit)

	// Start loader workers
	for i := 0; i < limit; i++ {
		eg.Go(func() error {
			defer loaderDone.Done()
			for blockTask := range blockTaskChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					// Load all blobs in this block
					for _, task := range blockTask.blobTasks {
						blob, err := p.cache.GetOrCompute(task.id, func() ([]byte, error) {
							return p.repo.LoadBlob(ctx, restic.DataBlob, task.id, nil)
						})
						if err != nil {
							return err
						}

						select {
						case loadedBlobChan <- LoadedBlob{data: blob, offset: task.offset}:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
			}
			return nil
		})
	}

	// Close loadedBlobChan when all loaders are done
	go func() {
		loaderDone.Wait()
		close(loadedBlobChan)
	}()

	// Start writer workers
	for i := 0; i < limit; i++ {
		eg.Go(func() error {
			for loadedBlob := range loadedBlobChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if _, err := w.WriteAt(loadedBlob.data, loadedBlob.offset); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	return eg.Wait()
}
