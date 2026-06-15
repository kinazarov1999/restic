package dump

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/restic/restic/internal/walker"
	"golang.org/x/sync/errgroup"
)

// A Dumper writes trees and files from a repository to a Writer
// in an archive format.
type Dumper interface {
	WriteNode(ctx context.Context, node *restic.Node) error
	DumpTree(ctx context.Context, tree *restic.Tree, rootPath string) error
}

// SequentialDumper writes trees and files sequentially.
type SequentialDumper struct {
	cache  *bloblru.Cache
	format string
	repo   restic.Loader
	writer io.Writer
}

type ParallelDumper struct {
	seq       *SequentialDumper
	writerAt  io.WriterAt
	skipZeros bool
	progress  *progress.Counter
}

func NewSequentialDumper(format string, repo restic.Loader, writer io.Writer) *SequentialDumper {
	return &SequentialDumper{
		cache:  bloblru.New(64 << 20),
		format: format,
		repo:   repo,
		writer: writer,
	}
}

func NewParallelDumper(seq *SequentialDumper, writerAt io.WriterAt, skipZeros bool, progress *progress.Counter) *ParallelDumper {
	return &ParallelDumper{
		seq:       seq,
		writerAt:  writerAt,
		skipZeros: skipZeros,
		progress:  progress,
	}
}

func (d *SequentialDumper) DumpTree(ctx context.Context, tree *restic.Tree, rootPath string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ch is buffered to deal with variable download/write speeds.
	ch := make(chan *restic.Node, 10)
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

func sendTrees(ctx context.Context, repo restic.BlobLoader, tree *restic.Tree, rootPath string, ch chan *restic.Node) {
	defer close(ch)

	for _, root := range tree.Nodes {
		root.Path = path.Join(rootPath, root.Name)
		if sendNodes(ctx, repo, root, ch) != nil {
			break
		}
	}
}

func sendNodes(ctx context.Context, repo restic.BlobLoader, root *restic.Node, ch chan *restic.Node) error {
	select {
	case ch <- root:
	case <-ctx.Done():
		return ctx.Err()
	}

	// If this is no directory we are finished
	if root.Type != restic.NodeTypeDir {
		return nil
	}

	err := walker.Walk(ctx, repo, *root.Subtree, walker.WalkVisitor{ProcessNode: func(_ restic.ID, nodepath string, node *restic.Node, err error) error {
		if err != nil {
			return err
		}
		if node == nil {
			return nil
		}

		node.Path = path.Join(root.Path, nodepath)

		if node.Type != restic.NodeTypeFile && node.Type != restic.NodeTypeDir && node.Type != restic.NodeTypeSymlink {
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
func (d *SequentialDumper) WriteNode(ctx context.Context, node *restic.Node) error {
	return d.writeNode(ctx, d.writer, node)
}

func (d *SequentialDumper) writeNode(ctx context.Context, w io.Writer, node *restic.Node) error {
	wg, ctx := errgroup.WithContext(ctx)
	limit := d.repo.Connections() - 1 // See below for the -1.
	blobs := make(chan (<-chan []byte), limit)

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
	// There will be at most 1+cap(blobs) calling LoadBlob at any moment.
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

func (p *ParallelDumper) WriteNode(ctx context.Context, node *restic.Node) error {
	return p.writeNode(ctx, p.writerAt, node)
}

func (p *ParallelDumper) DumpTree(ctx context.Context, tree *restic.Tree, rootPath string) error {
	return p.seq.DumpTree(ctx, tree, rootPath)
}

type BlobTask struct {
	id     restic.ID
	offset int64
}

func (p *ParallelDumper) writeNode(ctx context.Context, w io.WriterAt, node *restic.Node) error {
	wg, ctx := errgroup.WithContext(ctx)
	limit := int(p.seq.repo.Connections())
	wg.SetLimit(limit)

	if p.progress != nil {
		p.progress.SetMax(node.Size)
	}

	taskChan := make(chan BlobTask, limit*2)

	for i := 0; i < limit; i++ {
		wg.Go(func() error {
			for task := range taskChan {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					blob, err := p.seq.cache.GetOrCompute(task.id, func() ([]byte, error) {
						return p.seq.repo.LoadBlob(ctx, restic.DataBlob, task.id, nil)
					})
					if err != nil {
						return err
					}
					if _, err := w.WriteAt(blob, task.offset); err != nil {
						return err
					}
					if p.progress != nil {
						p.progress.Add(uint64(len(blob)))
					}
				}
			}
			return nil
		})
	}

	var currentOffset int64 = 0
	for _, id := range node.Content {
		size, found := p.seq.repo.LookupBlobSize(restic.DataBlob, id)
		if !found {
			return fmt.Errorf("blob %v not found", id)
		}
		if p.skipZeros && (id == repository.ZeroChunk()) {
			if p.progress != nil {
				p.progress.Add(uint64(size))
			}
			currentOffset += int64(size)
			continue
		}

		select {
		case taskChan <- BlobTask{
			id:     id,
			offset: currentOffset,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

		currentOffset += int64(size)
	}
	close(taskChan)

	return wg.Wait()
}
