package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/OpenSlides/openslides-icc-service/internal/icclog"
	"github.com/gomodule/redigo/redis"
)

const (
	// notifyKey is the name of the icc stream name.
	notifyKey = "icc-notify"

	// applauseKey is the name of the redis key for applause.
	applauseKey = "applause"
)

// Redis implements the icc backend by saving the data to redis.
//
// Has to be created with redis.New().
type Redis struct {
	pool         *redis.Pool
	lastNotifyID string
}

// New creates a new initializes redis instance.
func New(addr string) *Redis {
	return new(func() (redis.Conn, error) {
		return redis.Dial("tcp", addr)
	})
}

// NewByURL creates a new redis instance by a connection string DSN.
func NewByURL(url string) *Redis {
	return new(func() (redis.Conn, error) {
		return redis.DialURL(url)
	})
}

func new(dial func() (redis.Conn, error)) *Redis {
	pool := redis.Pool{
		MaxActive:   100,
		Wait:        true,
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial:        dial,
	}

	return &Redis{
		pool: &pool,
	}
}

// Wait blocks until a connection to redis can be established.
func (r *Redis) Wait(ctx context.Context) {
	for ctx.Err() == nil {
		conn := r.pool.Get()
		_, err := conn.Do("PING")
		conn.Close()
		if err == nil {
			return
		}
		icclog.Info("Waiting for redis: %v", err)
		time.Sleep(500 * time.Millisecond)
	}
}

// NotifyPublish saves a valid notify message.
func (r *Redis) NotifyPublish(message []byte) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("XADD", notifyKey, "*", "content", message)
	if err != nil {
		return fmt.Errorf("xadd: %w", err)
	}
	return nil
}

// NotifyReceive is a blocking function that receives the messages.
//
// The first call returnes the first notify message, the next call the second an
// so on. If there are no more messages to read, the function blocks until there
// is or the context ist canceled.
//
// It is expected, that only one goroutine is calling this function.
func (r *Redis) NotifyReceive(ctx context.Context) ([]byte, error) {
	id := r.lastNotifyID
	if id == "" {
		id = "$"
	}

	type streamReturn struct {
		id   string
		data []byte
		err  error
	}

	streamFinished := make(chan streamReturn)

	go func() {
		conn := r.pool.Get()
		defer conn.Close()

		id, data, err := stream(conn.Do("XREAD", "COUNT", 1, "BLOCK", "0", "STREAMS", notifyKey, id))
		streamFinished <- streamReturn{id, data, err}
	}()

	var received streamReturn
	select {
	case received = <-streamFinished:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if received.id != "" {
		r.lastNotifyID = id
	}

	if err := received.err; err != nil {
		return nil, fmt.Errorf("read notify message from redis: %w", err)
	}

	return received.data, nil
}

// ApplausePublish saves an applause for the user at a given time as unix time
// stamp.
func (r *Redis) ApplausePublish(meetingID, userID int, time int64) error {
	conn := r.pool.Get()
	defer conn.Close()

	meetingUser := fmt.Sprintf("%d-%d", meetingID, userID)
	if _, err := conn.Do("ZADD", applauseKey, time, meetingUser); err != nil {
		return fmt.Errorf("adding applause in redis: %w", err)
	}

	return nil
}

// ApplauseSince returned all applause since a given time as unix time stamp.
func (r *Redis) ApplauseSince(time int64) (map[int]int, error) {
	conn := r.pool.Get()
	defer conn.Close()

	meetingUsers, err := redis.Strings(conn.Do("ZRANGE", applauseKey, time, "+inf", "BYSCORE"))
	if err != nil {
		return nil, fmt.Errorf("getting applause from redis: %w", err)
	}

	out := make(map[int]int)
	for _, meetingUser := range meetingUsers {
		var meetingID int
		if _, err := fmt.Sscanf(meetingUser, "%d-", &meetingID); err != nil {
			return nil, fmt.Errorf("invalid value in redis %s: %w", meetingUser, err)
		}
		out[meetingID]++
	}

	return out, nil
}

// ApplauseCleanOld removes applause that is older then a given time.
func (r *Redis) ApplauseCleanOld(olderThen int64) error {
	conn := r.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("ZREMRANGEBYSCORE", applauseKey, 0, olderThen-1); err != nil {
		return fmt.Errorf("removing old applause from redis: %w", err)
	}
	return nil
}
