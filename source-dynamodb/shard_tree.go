package main

import (
	"sort"

	streamTypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type shardTree struct {
	shard    streamTypes.Shard
	children []*shardTree
}

// buildShardTrees returns a listing of trees, with the root node of each tree being a shard with no
// existing parent shard.
func buildShardTrees(shards map[string]streamTypes.Shard) []*shardTree {
	nodes := make(map[string]*shardTree)

	// Determine shard relationships.
	for shardId, shard := range shards {
		nodes[shardId] = &shardTree{
			shard: shard,
		}
	}

	// Root shards are those without a parent, or where the parent no longer exists.
	roots := []*shardTree{}

	for shardId, shard := range shards {
		hasParent := false

		if shard.ParentShardId != nil {
			// Add this shard as a child of its parent shard, if the parent shard is still around.
			if parent, ok := nodes[*shard.ParentShardId]; ok {
				hasParent = true
				parent.children = append(parent.children, nodes[shardId])
			}
		}

		if !hasParent {
			roots = append(roots, nodes[shardId])
		}
	}

	// Sort for a deterministic output.
	for _, n := range nodes {
		sort.Slice(n.children, func(i, j int) bool {
			return *n.children[i].shard.ShardId < *n.children[j].shard.ShardId
		})
	}
	sort.Slice(roots, func(i, j int) bool {
		return *roots[i].shard.ShardId < *roots[j].shard.ShardId
	})

	return roots
}
