package gitdag

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pransuu69/meritual-harness-webhook/internal/models"
)

// Resolver maintains an in-memory directed acyclic graph (DAG) of Git commits
// and reconstructs the exact chronological commit history.
type Resolver struct {
	mu sync.RWMutex

	// parentToChildren maps parent_sha -> slice of child commit_shas
	parentToChildren map[string][]string

	// childToParents maps child commit_sha -> slice of parent_shas
	childToParents map[string][]string

	// commits maps commit_sha -> Commit metadata
	commits map[string]models.Commit

	// roots holds known root commit SHAs
	roots map[string]struct{}

	// Async ingestion channel for zero lock-contention burst handling
	inboundChan chan models.Commit
	stopChan    chan struct{}
	drainWg     sync.WaitGroup

	// Cache resolved history until modified
	dirty           bool
	cachedHistory   []string
	cachedRootSHA   string
	cachedTipSHA    string
}

// NewResolver creates and initializes a thread-safe Git DAG Resolver.
func NewResolver(bufferSize int) *Resolver {
	if bufferSize <= 0 {
		bufferSize = 32768
	}
	r := &Resolver{
		parentToChildren: make(map[string][]string, 16384),
		childToParents:   make(map[string][]string, 16384),
		commits:          make(map[string]models.Commit, 16384),
		roots:            make(map[string]struct{}, 128),
		inboundChan:      make(chan models.Commit, bufferSize),
		stopChan:         make(chan struct{}),
		dirty:            true,
	}

	r.drainWg.Add(1)
	go r.ingestionWorker()

	return r
}

// IngestAsync enqueues a commit into the lock-free background ingestion channel.
func (r *Resolver) IngestAsync(c models.Commit) {
	r.inboundChan <- c
}

// IngestSync immediately adds a commit to the DAG under lock.
func (r *Resolver) IngestSync(c models.Commit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addCommitInternal(c)
}

// ingestionWorker continuously processes commits from the inbound channel in batches.
func (r *Resolver) ingestionWorker() {
	defer r.drainWg.Done()

	for {
		select {
		case c, ok := <-r.inboundChan:
			if !ok {
				return
			}
			r.mu.Lock()
			r.addCommitInternal(c)

			// Drain any additional pending commits in the channel in the same lock acquisition
		drainLoop:
			for {
				select {
				case nextCommit, more := <-r.inboundChan:
					if !more {
						break drainLoop
					}
					r.addCommitInternal(nextCommit)
				default:
					break drainLoop
				}
			}
			r.mu.Unlock()

		case <-r.stopChan:
			// Drain remaining commits before exiting
			r.mu.Lock()
			for len(r.inboundChan) > 0 {
				c := <-r.inboundChan
				r.addCommitInternal(c)
			}
			r.mu.Unlock()
			return
		}
	}
}

// addCommitInternal records a commit into the internal graph structure. (Caller must hold mu)
func (r *Resolver) addCommitInternal(c models.Commit) {
	if c.SHA == "" {
		return
	}

	if _, exists := r.commits[c.SHA]; exists {
		// Idempotent duplicate ingestion
		return
	}

	r.commits[c.SHA] = c
	r.dirty = true

	normParent := normalizeSHA(c.ParentSHA)

	if isRootParent(normParent) {
		r.roots[c.SHA] = struct{}{}
	} else {
		// Map parent -> child
		r.parentToChildren[normParent] = append(r.parentToChildren[normParent], c.SHA)
		// Map child -> parent
		r.childToParents[c.SHA] = append(r.childToParents[c.SHA], normParent)
	}
}

// Sync waits until all pending asynchronous commits are processed.
func (r *Resolver) Sync() {
	// Busy-wait or sleep until channel is empty and worker has processed batch
	for len(r.inboundChan) > 0 {
		time.Sleep(1 * time.Millisecond)
	}
	// Brief sleep to ensure worker lock cycle completes
	time.Sleep(2 * time.Millisecond)
}

// TotalCommits returns the current count of ingested valid commits.
func (r *Resolver) TotalCommits() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.commits)
}

// ResolveHistory computes the exact chronological commit history from root to tip.
// It executes a deterministic topological sort with timestamp / tie-break ordering.
func (r *Resolver) ResolveHistory() ([]string, string, string) {
	r.Sync()

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.dirty && r.cachedHistory != nil {
		return r.cachedHistory, r.cachedRootSHA, r.cachedTipSHA
	}

	n := len(r.commits)
	if n == 0 {
		return []string{}, "", ""
	}

	// 1. Identify all effective roots
	// An effective root is either explicitly marked as root or has a parent_sha not in r.commits.
	effectiveRoots := make([]string, 0, len(r.roots)+1)
	inDegree := make(map[string]int, n)

	for sha, c := range r.commits {
		normParent := normalizeSHA(c.ParentSHA)
		if isRootParent(normParent) {
			effectiveRoots = append(effectiveRoots, sha)
		} else if _, parentExists := r.commits[normParent]; !parentExists {
			// Parent was not in the ingested valid payloads: treat as an orphan/root
			effectiveRoots = append(effectiveRoots, sha)
		} else {
			inDegree[sha]++
		}
	}

	// If no roots detected (e.g. cycle or circular references), fallback to earliest timestamp commit
	if len(effectiveRoots) == 0 {
		var earliestSHA string
		var earliestTime time.Time
		for sha, c := range r.commits {
			if earliestSHA == "" || c.Timestamp.Before(earliestTime) {
				earliestSHA = sha
				earliestTime = c.Timestamp
			}
		}
		if earliestSHA != "" {
			effectiveRoots = append(effectiveRoots, earliestSHA)
		}
	}

	// Sort initial roots deterministically by timestamp ascending, then lexicographical SHA
	r.sortCommitList(effectiveRoots)

	// 2. Kahn's algorithm for topological sorting
	ordered := make([]string, 0, n)
	queue := make([]string, 0, len(effectiveRoots))
	queue = append(queue, effectiveRoots...)

	visited := make(map[string]struct{}, n)

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		if _, alreadyVisited := visited[curr]; alreadyVisited {
			continue
		}
		visited[curr] = struct{}{}
		ordered = append(ordered, curr)

		children := r.parentToChildren[curr]
		if len(children) > 1 {
			// Deterministic branch traversal
			r.sortCommitList(children)
		}

		for _, child := range children {
			inDegree[child]--
			if inDegree[child] <= 0 {
				queue = append(queue, child)
			}
		}
	}

	// 3. Collect any disjoint commits not reached by traversal (defensive for fragmented trees)
	if len(ordered) < n {
		unvisited := make([]string, 0, n-len(ordered))
		for sha := range r.commits {
			if _, ok := visited[sha]; !ok {
				unvisited = append(unvisited, sha)
			}
		}
		r.sortCommitList(unvisited)
		ordered = append(ordered, unvisited...)
	}

	var rootSHA, tipSHA string
	if len(ordered) > 0 {
		rootSHA = ordered[0]
		tipSHA = ordered[len(ordered)-1]
	}

	r.cachedHistory = ordered
	r.cachedRootSHA = rootSHA
	r.cachedTipSHA = tipSHA
	r.dirty = false

	return ordered, rootSHA, tipSHA
}

// Reset clears the DAG state.
func (r *Resolver) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.parentToChildren = make(map[string][]string, 16384)
	r.childToParents = make(map[string][]string, 16384)
	r.commits = make(map[string]models.Commit, 16384)
	r.roots = make(map[string]struct{}, 128)
	r.cachedHistory = nil
	r.cachedRootSHA = ""
	r.cachedTipSHA = ""
	r.dirty = true

	// Clear inbound channel
	for len(r.inboundChan) > 0 {
		<-r.inboundChan
	}
}

// Close shuts down the worker goroutine.
func (r *Resolver) Close() {
	close(r.stopChan)
	r.drainWg.Wait()
}

func (r *Resolver) sortCommitList(list []string) {
	sort.Slice(list, func(i, j int) bool {
		cI := r.commits[list[i]]
		cJ := r.commits[list[j]]
		if !cI.Timestamp.Equal(cJ.Timestamp) {
			return cI.Timestamp.Before(cJ.Timestamp)
		}
		return list[i] < list[j]
	})
}

func normalizeSHA(sha string) string {
	return strings.ToLower(strings.TrimSpace(sha))
}

func isRootParent(parent string) bool {
	if parent == "" {
		return true
	}
	// Check for zero SHAs: 40 chars or 64 chars of '0'
	for i := 0; i < len(parent); i++ {
		if parent[i] != '0' {
			return false
		}
	}
	return true
}
