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
	redisClient    *redis.Client
	name           string
	rankSet        string
	memberScoreSet string
	opts           *Options
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

	addMember := redis.NewScript(`
local key = KEYS[1]
local member_id = ARGV[1]
local change = ARGV[2]

local value = redis.call("GET", key)
if not value then
  value = 0
end

value = value + change
redis.call("SET", key, value)

return value
`)

	return &leaderboard{
		redisClient:    redisClient,
		name:           name,
		rankSet:        rankSet,
		memberScoreSet: memberScoreSet,
		opts:           opts,
	}
}

func generateRankSetName(name string) string {
	return fmt.Sprintf("goleaderboard:%s:rank_set", name)
}

func genenrateMemScoreSetName(name string) string {
	return fmt.Sprintf("goleaderboard:%s:member_score_set", name)
}

func (l *leaderboard) AddMember(ctx context.Context, member *Member) error {
	pipeline := l.redisClient.Pipeline()
	oldScore := pipeline.ZScore(ctx, l.memberScoreSet, fmt.Sprintf("%v", member.ID))

	pipeline.ZAdd(ctx, l.rankSet, &redis.Z{
		Score:  float64(member.Score),
		Member: member.Score,
	})
	pipeline.ZAdd(ctx, l.memberScoreSet, &redis.Z{
		Score:  float64(member.Score),
		Member: member.ID,
	})

	minOldScoreVal := fmt.Sprintf("%f", oldScore.Val()-1)
	oldScoreVal := fmt.Sprintf("%f", oldScore.Val())
	countMemberWithOldScore := pipeline.ZCount(ctx, l.memberScoreSet, minOldScoreVal, oldScoreVal)
	if countMemberWithOldScore.Val() == 0 {
		pipeline.ZRem(ctx, l.rankSet, oldScoreVal)
	}

	if _, err := pipeline.Exec(ctx); err != nil {
		return err
	}

	return nil
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
