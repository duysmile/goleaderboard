package goleaderboard

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Options struct {
	AllowSameRank bool
	LifeTime      time.Duration
}

type Member struct {
	ID    interface{}
	Score int
}

type Leaderboard interface {
	AddMember(ctx context.Context, member *Member) error
	List(ctx context.Context, offset, limit int) ([]*Member, error)
	GetAround(ctx context.Context, member *Member) ([]*Member, error)
	GetRank(ctx context.Context, member *Member) (int, error)
}

type leaderboard struct {
	redisClient     *redis.Client
	name            string
	rankSet         string
	memberScoreSet  string
	addMemberScript *redis.Script
	opts            *Options
}

func NewLeaderBoard(redisClient *redis.Client, name string, opts *Options) Leaderboard {
	if opts == nil {
		opts = &Options{
			AllowSameRank: false,
			LifeTime:      1 * time.Hour,
		}
	}
	rankSet := generateRankSetName(name)
	memberScoreSet := genenrateMemScoreSetName(name)

	lb := &leaderboard{
		redisClient:    redisClient,
		name:           name,
		rankSet:        rankSet,
		memberScoreSet: memberScoreSet,
		opts:           opts,
	}

	lb.addMemberScript = redis.NewScript(`
local key = KEYS[1]
local member_id = ARGV[1]
local new_score = ARGV[2]

local member_score_set = "goleaderboard:" .. key .. ":member_score_set"
local rank_set = "goleaderboard:" .. key .. ":rank_set"

local old_score = redis.call("ZSCORE", member_score_set, member_id)
	
redis.call("ZADD", rank_set, new_score, new_score)
redis.call("ZADD", member_score_set, new_score, member_id)

local count_member_in_old_score = redis.call("ZCOUNT", member_score_set, old_score, old_score)
if count_member_in_old_score == 0 then
	redis.call("ZREM", rank_set, old_score)
end

return 1
`)

	return lb
}

func generateRankSetName(name string) string {
	return fmt.Sprintf("goleaderboard:%s:rank_set", name)
}

func genenrateMemScoreSetName(name string) string {
	return fmt.Sprintf("goleaderboard:%s:member_score_set", name)
}

func (l *leaderboard) AddMember(ctx context.Context, member *Member) error {
	return l.addMemberScript.Run(ctx, l.redisClient, []string{l.name}, member.ID, member.Score).Err()
}

func (l *leaderboard) List(ctx context.Context, offset, limit int) ([]*Member, error) {
	panic("implement me")
}

func (l *leaderboard) GetAround(ctx context.Context, member *Member) ([]*Member, error) {
	panic("implement me")
}

func (l *leaderboard) GetRank(ctx context.Context, member *Member) (int, error) {
	panic("implement me")
}
