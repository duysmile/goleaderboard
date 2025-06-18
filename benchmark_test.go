package goleaderboard

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

func benchmarkSetup(b *testing.B) (*redis.Client, Leaderboard, context.Context) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("you must start redis server: %v", err)
	}
	lb := NewLeaderBoard(rdb, fmt.Sprintf("bench_%d", b.N), &Options{AllowSameRank: true})
	ctx := context.Background()
	return rdb, lb, ctx
}

func benchmarkTeardown(b *testing.B, rdb *redis.Client, lb Leaderboard, ctx context.Context) {
	lb.Clean(ctx)
	rdb.Close()
}

func BenchmarkAddMember(b *testing.B) {
	rdb, lb, ctx := benchmarkSetup(b)
	defer benchmarkTeardown(b, rdb, lb, ctx)

	// Prepopulate leaderboard with a large number of members
	const preMembers = 10000
	for i := 0; i < preMembers; i++ {
		lb.AddMember(ctx, fmt.Sprintf("pre_%d", i), preMembers-i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lb.AddMember(ctx, fmt.Sprintf("bm_%d", i), i)
	}
}

func BenchmarkGetAround(b *testing.B) {
	rdb, lb, ctx := benchmarkSetup(b)
	defer benchmarkTeardown(b, rdb, lb, ctx)

	const members = 10000
	for i := 0; i < members; i++ {
		lb.AddMember(ctx, fmt.Sprintf("m_%d", i), members-i)
	}
	target := fmt.Sprintf("m_%d", members/2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lb.GetAround(ctx, target, 10, OrderDesc)
	}
}
