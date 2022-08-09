# goleaderboard
Package to make your own leaderboard in simple way

## What is it?
A leaderboard written in Go using Redis database

## Features
- Ranking members by score
- Members with same score will have the same rank
- Get around members of a member with specific order
- Can create multiple leaderboards by name

## Installation
Install by using `go get`
```bash
go get github.com/duysmile/goleaderboard
```

## How to use

Create a new leaderboard
```go
rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
leaderboard := goleaderboard.NewLeaderBoard(rdb, "test", nil)
```

Add a member with `id` and `score`
```go
leaderboard.AddMember(ctx, "P4", 2)
```

Get rank of a member by `id`
```go
rank, _ := leaderboard.GetRank(ctx, "P4")
fmt.Println("rank of member:", fmt.Sprintf("#%v", rank))
```

List members by rank
```go
list, _ := leaderboard.List(ctx, 0, 10, goleaderboard.OrderDesc)

// you can choose the order you want
// for example: 
// list, _ := leaderboard.List(ctx, 0, 10, goleaderboard.OrderAsc)
```

Get around of a member
```go
list, _ = leaderboard.GetAround(ctx, "P4", 4, goleaderboard.OrderDesc)
```

## Contribution
All your contributions to project and make it better, they are welcome. Feel free to start an [issue](https://github.com/duysmile/goleaderboard/issues).

## License
@2022, DuyN. Released under the [MIT License](https://github.com/duysmile/goleaderboard/blob/master/LICENSE)