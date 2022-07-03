package batch

import (
	"context"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

type user struct {
	ID int64
}

func getOne(id int64) user {
	time.Sleep(time.Millisecond * 100)
	return user{ID: id}
}

func getBatch(n int64, pool int64) (res []user) {
	//return getBatchWorkerPool(n, pool)
	return getBatchErrGroup(n, pool)
}

func getBatchErrGroup(n int64, pool int64) (res []user) {
	errG, _ := errgroup.WithContext(context.Background())
	errG.SetLimit(int(pool))

	var mu sync.Mutex

	for i := int64(0); i < n; i++ {
		id := i
		errG.Go(func() error {
			ans := getOne(id)
			mu.Lock()
			defer mu.Unlock()
			res = append(res, ans)
			return nil
		})
	}

	_ = errG.Wait()
	return
}

func getBatchWorkerPool(n int64, pool int64) (res []user) {
	pendingJobs := make(chan int64, n/2)
	finishedJobs := make(chan user, n/2)

	for i := int64(0); i < pool; i++ {
		go func() {
			for j := range pendingJobs {
				finishedJobs <- getOne(j)
			}
		}()
	}

	for i := int64(0); i < n; i++ {
		pendingJobs <- i
	}
	close(pendingJobs)

	for i := int64(0); i < n; i++ {
		res = append(res, <-finishedJobs)
	}

	return
}
