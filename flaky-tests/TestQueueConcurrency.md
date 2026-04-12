# Flaky Test: TestQueueConcurrency

**Status**: Active
**Occurrences**: 1
**Root Cause**: Deadlock/hang in concurrent test. The test spawns 30 goroutines that concurrently enqueue, dequeue, and delete from a queue. Goroutines where `cnt%5 == 0` (and odd) call `dequeueRequest(0, false)` which blocks on a channel waiting for an item. If other goroutines have already drained the queue or called `deleteQueue`, the dequeue goroutine blocks forever — there will never be another enqueue to unblock it. This causes the entire test to hang until the 30-minute timeout. The issue manifests more on arm64 with `-race` due to slower execution and different goroutine scheduling.

## Occurrences

### 2026-04-12T20:23:09Z
- **Job**: [ci / test (arm64)](https://github.com/cortexproject/cortex/actions/runs/24314927948)
- **Package**: `github.com/cortexproject/cortex/pkg/scheduler/queue`
- **File**: `pkg/scheduler/queue/user_queues_test.go:461`
- **Notes**: Timed out after 30m on arm64 with `-race`. Passed on amd64 with `-race`, passed on both arches without `-race`. Goroutine dump shows `dequeueRequest` stuck waiting on channel at `user_request_queue.go:35`.

<details><summary>Build logs</summary>

```
panic: test timed out after 30m0s
	running tests:
		TestQueueConcurrency (29m51s)

goroutine 100 [chan receive, 29 minutes]:
github.com/cortexproject/cortex/pkg/scheduler/queue.(*FIFORequestQueue).dequeueRequest(0xc000410080, 0xc8ea31?, 0x6?)
	/__w/cortex/cortex/pkg/scheduler/queue/user_request_queue.go:35 +0x48
github.com/cortexproject/cortex/pkg/scheduler/queue.TestQueueConcurrency.func1(0xf)
	/__w/cortex/cortex/pkg/scheduler/queue/user_queues_test.go:477 +0x280

FAIL	github.com/cortexproject/cortex/pkg/scheduler/queue	1800.101s
```

</details>
