package main

import (
	"context"
	"fmt"

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
	leaderboard.AddMember(ctx, "1", 1)
	leaderboard.AddMember(ctx, "2", 2)
	leaderboard.AddMember(ctx, "3", 2)
	leaderboard.AddMember(ctx, "4", 3)

	// change score of member "4"
	leaderboard.AddMember(ctx, "4", 2)

	// list member with rank
	list, _ := leaderboard.List(ctx, 0, 10, goleaderboard.OrderDesc)

	for idx := range list {
		fmt.Println("list member  :", list[idx].ID, list[idx].Score, list[idx].Rank)
	}

	// get rank of member
	rank, _ := leaderboard.GetRank(ctx, "2")
	fmt.Println("rank of member 2", rank)
}
