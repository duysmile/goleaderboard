package goleaderboard

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type Options struct {
	AllowSameRank bool
	LifeTime      time.Duration
}

type Order string

var (
	OrderDesc Order = "desc"
	OrderAsc  Order = "asc"
)

type Member struct {
	ID    interface{}
	Score int
	Rank  int
}

type Leaderboard interface {
	AddMember(ctx context.Context, id interface{}, score int) error
	List(ctx context.Context, offset, limit int, order Order) ([]*Member, error)
	GetAround(ctx context.Context, id interface{}, limit int, order Order) ([]*Member, error)
	GetRank(ctx context.Context, id interface{}) (int, error)
	Clean(ctx context.Context) error
}

type leaderboard struct {
	redisClient      *redis.Client
	name             string
	rankSet          string
	memberScoreSet   string
	addMemberScript  *redis.Script
	listMemberScript *redis.Script
	getRankScript    *redis.Script
	getAroundScript  *redis.Script
	opts             *Options
}

func NewLeaderBoard(redisClient *redis.Client, name string, opts *Options) Leaderboard {
	if opts == nil {
		opts = &Options{
			AllowSameRank: false,
			LifeTime:      1 * time.Hour,
		}
	}
	rankSet := generateRankSetName(name)
	memberScoreSet := generateMemScoreSetName(name)

	lb := &leaderboard{
		redisClient:    redisClient,
		name:           name,
		rankSet:        rankSet,
		memberScoreSet: memberScoreSet,
		opts:           opts,
	}

	lb.addMemberScript = initAddMemberScript()
	lb.listMemberScript = initGetListMemberWithRankScript()
	lb.getRankScript = initGetRankScript()
	lb.getAroundScript = initGetAroundScript()

	return lb
}

func (l *leaderboard) AddMember(ctx context.Context, id interface{}, score int) error {
	_, err := l.addMemberScript.Run(ctx, l.redisClient, []string{l.name}, id, score).Result()
	return err
}

func (l *leaderboard) _List(ctx context.Context, offset, limit int, order Order) ([]*Member, error) {
	cmd := l.redisClient.ZRevRangeWithScores
	if order == OrderAsc {
		cmd = l.redisClient.ZRangeWithScores
	}
	listMemberRedis, err := cmd(
		ctx,
		generateMemScoreSetName(l.name),
		int64(offset),
		int64(offset+limit),
	).Result()

	if err != nil {
		return nil, err
	}

	pipeline := l.redisClient.Pipeline()

	uniqueScores := make(map[float64]*redis.IntCmd)
	for _, member := range listMemberRedis {
		if _, ok := uniqueScores[member.Score]; ok {
			continue
		}

		rankCmd := pipeline.ZRevRank(
			ctx,
			generateRankSetName(l.name),
			fmt.Sprintf("%v", member.Score),
		)
		uniqueScores[member.Score] = rankCmd
	}

	if _, err := pipeline.Exec(ctx); err != nil {
		return nil, err
	}

	listMember := make([]*Member, 0, len(listMemberRedis))
	for _, member := range listMemberRedis {
		mem := &Member{
			ID:    member.Member,
			Score: int(member.Score),
			Rank:  int(uniqueScores[member.Score].Val()) + 1,
		}
		listMember = append(listMember, mem)
	}

	return listMember, nil
}

func (l *leaderboard) List(ctx context.Context, offset, limit int, order Order) ([]*Member, error) {
	listMemberRankTmp, err := l.listMemberScript.Run(ctx, l.redisClient, []string{l.name}, offset, limit, string(order)).Result()
	if err != nil {
		return nil, err
	}

	listMemberRank := listMemberRankTmp.([]interface{})
	listMember := make([]*Member, len(listMemberRank)/3)
	for idx, val := range listMemberRank {
		if idx%3 == 1 {
			listMember[idx/3].Score = interfaceToInt(val)
		} else if idx%3 == 2 {
			listMember[idx/3].Rank = interfaceToInt(val)
		} else {
			listMember[idx/3] = &Member{
				ID: val,
			}
		}
	}

	return listMember, nil
}

func (l *leaderboard) _GetAround(ctx context.Context, id interface{}, limit int, order Order) ([]*Member, error) {
	rankCmd := l.redisClient.ZRevRank
	if order == OrderAsc {
		rankCmd = l.redisClient.ZRank
	}
	rank, err := rankCmd(
		ctx,
		generateMemScoreSetName(l.name),
		fmt.Sprintf("%v", id),
	).Result()

	if err != nil {
		return nil, err
	}

	total, err := l.redisClient.ZCount(
		ctx,
		generateMemScoreSetName(l.name),
		"-inf",
		"+inf",
	).Result()

	if err != nil {
		return nil, err
	}

	start := cursorAround(
		int(rank),
		limit,
		int(total),
	)

	return l.List(ctx, start, limit, order)
}

func (l *leaderboard) GetAround(ctx context.Context, id interface{}, limit int, order Order) ([]*Member, error) {
	listMemberRankTmp, err := l.getAroundScript.Run(ctx, l.redisClient, []string{l.name}, id, limit, string(order)).Result()
	if err != nil {
		return nil, err
	}

	listMemberRank := listMemberRankTmp.([]interface{})
	listMember := make([]*Member, len(listMemberRank)/3)
	for idx, val := range listMemberRank {
		if idx%3 == 1 {
			listMember[idx/3].Score = interfaceToInt(val)
		} else if idx%3 == 2 {
			listMember[idx/3].Rank = interfaceToInt(val)
		} else {
			listMember[idx/3] = &Member{
				ID: val,
			}
		}
	}

	return listMember, nil
}

func (l *leaderboard) _GetRank(ctx context.Context, id interface{}) (int, error) {
	score, err := l.redisClient.ZScore(
		ctx,
		generateMemScoreSetName(l.name),
		fmt.Sprintf("%s", id),
	).Result()

	if err != nil {
		return 0, err
	}

	rank, err := l.redisClient.ZRevRank(
		ctx,
		generateRankSetName(l.name),
		fmt.Sprintf("%v", score),
	).Result()

	if err != nil {
		return 0, err
	}

	return int(rank) + 1, nil
}

func (l *leaderboard) GetRank(ctx context.Context, id interface{}) (int, error) {
	rankData, err := l.getRankScript.Run(ctx, l.redisClient, []string{l.name}, id).Result()
	if err != nil {
		return 0, nil
	}

	rank := rankData.(int64)
	return int(rank), nil
}

func (l *leaderboard) Clean(ctx context.Context) error {
	pipeline := l.redisClient.Pipeline()
	pipeline.Del(ctx, generateRankSetName(l.name))
	pipeline.Del(ctx, generateMemScoreSetName(l.name))

	_, err := pipeline.Exec(ctx)
	return err
}

func initAddMemberScript() *redis.Script {
	return redis.NewScript(`
local key = KEYS[1]
local member_id = ARGV[1]
local new_score = ARGV[2]

local member_score_set = "goleaderboard:" .. key .. ":member_score_set"
local rank_set = "goleaderboard:" .. key .. ":rank_set"

local old_score = redis.call("ZSCORE", member_score_set, member_id)
	
redis.call("ZADD", rank_set, new_score, new_score)
redis.call("ZADD", member_score_set, new_score, member_id)

if old_score ~= nil then
	return 1
end

local count_member_in_old_score = redis.call("ZCOUNT", member_score_set, old_score, old_score)
if count_member_in_old_score == 0 then
	redis.call("ZREM", rank_set, old_score)
end

return 1
`)
}

func initGetListMemberWithRankScript() *redis.Script {
	return redis.NewScript(`
local key = KEYS[1]
local offset = ARGV[1]
local limit = ARGV[2]
local order = ARGV[3]

local member_score_set = "goleaderboard:" .. key .. ":member_score_set"
local rank_set = "goleaderboard:" .. key .. ":rank_set"

local listCmd = "ZREVRANGE"
if order == "asc" then
	listCmd = "ZRANGE"
end

local list_member_with_score = redis.call(listCmd, member_score_set, offset, offset + limit - 1, "WITHSCORES")

local list_member_with_rank = {}

for idx,val in ipairs(list_member_with_score) do
	table.insert(list_member_with_rank, val) 

	if idx % 2 == 0 then 
		local rank = redis.call("ZREVRANK", rank_set, val)
		table.insert(list_member_with_rank, tostring(rank + 1))
	end
end

return list_member_with_rank
`)
}

func initGetRankScript() *redis.Script {
	return redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]

local member_score_set = "goleaderboard:" .. key .. ":member_score_set"
local rank_set = "goleaderboard:" .. key .. ":rank_set"

local score = redis.call("ZSCORE", member_score_set, id)
local rank = redis.call("ZREVRANK", rank_set, tostring(score))

if not rank then
	return redis.call("ZCOUNT", rank_set, "-inf", "+inf") + 1
end

return rank + 1
`)
}

func initGetAroundScript() *redis.Script {
	return redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]
local limit = ARGV[2]
local order = ARGV[3]

local member_score_set = "goleaderboard:" .. key .. ":member_score_set"
local rank_set = "goleaderboard:" .. key .. ":rank_set"

local rankCmd = "ZREVRANK"
if order == "asc" then
	rankCmd = "ZRANK"
end

local rank = redis.call(rankCmd, member_score_set, id)
local total = redis.call("ZCOUNT", member_score_set, "-inf", "+inf")

local offset = rank - math.floor(limit/2)
if offset < 0 then
	offset = 0
end
local remain = offset + limit - total
if remain > 0 then
	offset = offset - remain
end

if offset < 0 then
	offset = 0
end

local listCmd = "ZREVRANGE"
if order == "asc" then
	listCmd = "ZRANGE"
end

local list_member_with_score = redis.call(listCmd, member_score_set, offset, offset + limit - 1, "WITHSCORES")

local list_member_with_rank = {}

for idx,val in ipairs(list_member_with_score) do
	table.insert(list_member_with_rank, val) 

	if idx % 2 == 0 then 
		local rank = redis.call("ZREVRANK", rank_set, val)
		table.insert(list_member_with_rank, tostring(rank + 1))
	end
end

return list_member_with_rank
`)
}

func generateRankSetName(name string) string {
	return fmt.Sprintf("goleaderboard:%s:rank_set", name)
}

func generateMemScoreSetName(name string) string {
	return fmt.Sprintf("goleaderboard:%s:member_score_set", name)
}

func interfaceToInt(val interface{}) int {
	str := fmt.Sprintf("%s", val)
	v, _ := strconv.ParseInt(str, 10, 64)
	return int(v)
}

func maxInt(nums ...int) int {
	if len(nums) == 0 {
		return 0
	}
	max := nums[0]
	for _, n := range nums {
		if n > max {
			max = n
		}
	}
	return max
}

func cursorAround(rank, limit, total int) (start int) {
	start = maxInt(rank-limit/2, 0)
	remain := start + limit - total
	if remain > 0 {
		start = maxInt(0, start-remain)
	}
	return
}
