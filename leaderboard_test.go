package goleaderboard

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
)

var (
	redisClient *redis.Client
)

func setup(t *testing.T) {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		t.Fatal("you must start redis server", err)
	}
}

func teardown(t *testing.T) {
	if err := redisClient.Close(); err != nil {
		t.Fatal(err)
	}
}

func clean(t *testing.T, ctx context.Context, leader Leaderboard) {
	if err := leader.Clean(ctx); err != nil {
		t.Fatal("failed to clean leaderboard", err.Error())
	}
}

func initLeaderboard(
	t *testing.T,
	ctx context.Context,
	numberOfMember int,
	opts *Options,
) Leaderboard {
	leaderboard := NewLeaderBoard(redisClient, "test", opts)
	for i := 0; i < numberOfMember; i++ {
		id := fmt.Sprintf("P%v", i)
		addMember(t, ctx, leaderboard, id, numberOfMember-i)
	}

	return leaderboard
}

func getRank(
	t *testing.T,
	ctx context.Context,
	leaderboard Leaderboard,
	id interface{},
	expected int,
) {
	rank, err := leaderboard.GetRank(ctx, id)
	if err != nil {
		t.Error("failed to get rank of member", err.Error())
		return
	}

	if rank != expected {
		t.Errorf("Error in get rank of member\nExpected: rank #%v\nReceived: rank #%v", expected, rank)
	}
}

func addMember(t *testing.T, ctx context.Context, leaderboard Leaderboard, id interface{}, score int) {
	err := leaderboard.AddMember(ctx, id, score)
	if err != nil {
		t.Error("failed to add member", err.Error())
		return
	}
}

func getAround(
	t *testing.T,
	ctx context.Context,
	leaderboard Leaderboard,
	id interface{},
	limit int,
	expected int,
) ([]*Member, Cursor) {
	list, cursor, err := leaderboard.GetAround(ctx, id, limit, OrderDesc)
	if err != nil {
		t.Error("failed to get around", err.Error())
		return nil, Cursor{}
	}

	if len(list) != expected {
		t.Errorf("Error in get around\nExpected: %v\nReceived: %v", limit, len(list))
		return nil, Cursor{}
	}

	return list, cursor
}

func TestAddMember(t *testing.T) {
	setup(t)
	defer teardown(t)

	testCases := []Options{
		{
			AllowSameRank: false,
		},
		{
			AllowSameRank: true,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		numberOfMember := 10
		leaderboard := initLeaderboard(t, ctx, numberOfMember, &tc)
		defer clean(t, ctx, leaderboard)

		members, cursor, err := leaderboard.List(ctx, 0, numberOfMember, OrderDesc)
		if err != nil {
			t.Error("failed to list members", err.Error())
			return
		}

		if cursor.Begin != 0 {
			t.Errorf("something went wrong when list member\nExpected: cursor begins at %v\nReceived: cursor begins at %v", cursor.Begin, 0)
		}

		if cursor.End != numberOfMember {
			t.Errorf("something went wrong when list member\nExpected: cursor ends at %v\nReceived: cursor ends at %v", cursor.End, numberOfMember)
		}

		if len(members) != numberOfMember {
			t.Errorf("something went wrong when add member\nExpected: adding %v members\nReceived: adding %v members", numberOfMember, len(members))
		}

		clean(t, ctx, leaderboard)
	}
}

func TestRankingMember(t *testing.T) {
	setup(t)
	defer teardown(t)

	testCases := []Options{
		{
			AllowSameRank: false,
		},
		{
			AllowSameRank: true,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		numberOfMember := 10
		leaderboard := initLeaderboard(t, ctx, numberOfMember, &tc)
		defer clean(t, ctx, leaderboard)

		player := "PMax"
		addMember(t, ctx, leaderboard, player, 100)
		getRank(t, ctx, leaderboard, player, 1)

		playerMaxMax := "PMaxMax"
		addMember(t, ctx, leaderboard, playerMaxMax, 1000)
		getRank(t, ctx, leaderboard, player, 2)
		clean(t, ctx, leaderboard)
	}
}

func TestSameRankingMember(t *testing.T) {
	setup(t)
	defer teardown(t)

	ctx := context.Background()
	numberOfMember := 10
	leaderboard := initLeaderboard(t, ctx, numberOfMember, &Options{AllowSameRank: true})
	defer clean(t, ctx, leaderboard)

	player1 := "PSame1"
	err := leaderboard.AddMember(ctx, player1, 10)
	if err != nil {
		t.Error("failed to add member", err.Error())
		return
	}

	player2 := "PSame2"
	err = leaderboard.AddMember(ctx, player2, 10)
	if err != nil {
		t.Error("failed to add member", err.Error())
		return
	}

	getRank(t, ctx, leaderboard, player1, 1)
	getRank(t, ctx, leaderboard, player2, 1)
}

func TestGetAround(t *testing.T) {
	setup(t)
	defer teardown(t)

	testCases := []Options{
		{
			AllowSameRank: false,
		},
		{
			AllowSameRank: true,
		},
	}

	for _, tc := range testCases {
		ctx := context.Background()
		numberOfMember := 10
		leaderboard := initLeaderboard(t, ctx, numberOfMember, &tc)
		defer clean(t, ctx, leaderboard)

		player := "P4"
		limit := 1
		list, _ := getAround(t, ctx, leaderboard, player, limit, limit)
		if list != nil && list[0].ID != player {
			t.Errorf("Error in get around of member with limit %v\nExpected: player %v\nReceived: player %v", limit, player, list[0].ID)
			return
		}

		limit = 6
		list, _ = getAround(t, ctx, leaderboard, player, limit, limit)
		if list != nil && list[limit/2].ID != player {
			t.Errorf("Error in get around of member with limit %v\nExpected: player %v\nReceived: player %v", limit, player, list[limit/2].ID)
			return
		}

		player = "P0"
		list, _ = getAround(t, ctx, leaderboard, player, limit, limit)
		if list != nil && list[0].ID != player {
			t.Errorf("Error in get around of member with player in top 1\nExpected: player %v\nReceived: player %v", player, list[0].ID)
			return
		}

		player = "P9"
		list, _ = getAround(t, ctx, leaderboard, player, limit, limit)
		if list != nil && list[limit-1].ID != player {
			t.Errorf("Error in get around of member with player in top 10\nExpected: player %v\nReceived: player %v", player, list[limit-1].ID)
			return
		}
		clean(t, ctx, leaderboard)
	}
}
