package main

import (
	"context"

	"github.com/duysmile/goleaderboard"
	"github.com/go-redis/redis/v8"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	leaderboard := goleaderboard.NewLeaderBoard(rdb, "test", nil)

	ctx := context.Background()
	//leaderboard.AddMember(ctx, &goleaderboard.Member{
	//	ID:    "1",
	//	Score: 1,
	//})
	//leaderboard.AddMember(ctx, &goleaderboard.Member{
	//	ID:    "2",
	//	Score: 2,
	//})
	//leaderboard.AddMember(ctx, &goleaderboard.Member{
	//	ID:    "3",
	//	Score: 2,
	//})
	leaderboard.AddMember(ctx, &goleaderboard.Member{
		ID:    "4",
		Score: 3,
	})
}
