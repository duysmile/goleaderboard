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
	leaderboard.AddMember(ctx, "P1", 9)
	leaderboard.AddMember(ctx, "P2", 8)
	leaderboard.AddMember(ctx, "P3", 7)
	leaderboard.AddMember(ctx, "P4", 6)
	leaderboard.AddMember(ctx, "P5", 5)
	leaderboard.AddMember(ctx, "P6", 4)
	leaderboard.AddMember(ctx, "P7", 3)
	leaderboard.AddMember(ctx, "P8", 2)
	leaderboard.AddMember(ctx, "P9", 1)

	// change score of member "4"
	leaderboard.AddMember(ctx, "P4", 2)

	// list member with rank
	list, _ := leaderboard.List(ctx, 0, 10, goleaderboard.OrderDesc)
	Print(list)

	// get rank of member
	rank, _ := leaderboard.GetRank(ctx, "P2")
	fmt.Println("rank of member 2:", fmt.Sprintf("#%v", rank))

	// get around a member
	list, _ = leaderboard.GetAround(ctx, "P4", 4, goleaderboard.OrderDesc)
	Print(list)
}

func Print(list []*goleaderboard.Member) {
	fmt.Println("==== Leaderboard ====")
	fmt.Println("# || ID || Score")
	for idx := range list {
		fmt.Println(list[idx].Rank, "|| ", list[idx].ID, "||", list[idx].Score)
	}
	fmt.Println("=====================")
}
