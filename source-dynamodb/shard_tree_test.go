package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamTypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/require"
)

func TestBuildShardTrees(t *testing.T) {
	nilParent := streamTypes.Shard{
		ParentShardId: nil,
		ShardId:       aws.String("nilParent"),
	}
	child := streamTypes.Shard{
		ParentShardId: nilParent.ShardId,
		ShardId:       aws.String("child"),
	}
	absentParent := streamTypes.Shard{
		ParentShardId: aws.String("notHere"),
		ShardId:       aws.String("absentParent"),
	}
	sibling1 := streamTypes.Shard{
		ParentShardId: absentParent.ShardId,
		ShardId:       aws.String("sibling1"),
	}
	sibling2 := streamTypes.Shard{
		ParentShardId: absentParent.ShardId,
		ShardId:       aws.String("sibling2"),
	}
	childOfSibling1 := streamTypes.Shard{
		ParentShardId: sibling1.ShardId,
		ShardId:       aws.String("childOfSibling1"),
	}

	shards := map[string]streamTypes.Shard{
		*nilParent.ShardId:       nilParent,
		*absentParent.ShardId:    absentParent,
		*child.ShardId:           child,
		*sibling1.ShardId:        sibling1,
		*sibling2.ShardId:        sibling2,
		*childOfSibling1.ShardId: childOfSibling1,
	}

	expect1 := shardTree{
		shard: absentParent,
		children: []*shardTree{
			{
				shard: sibling1,
				children: []*shardTree{
					{
						shard:    childOfSibling1,
						children: nil,
					},
				},
			},
			{
				shard:    sibling2,
				children: nil,
			},
		},
	}

	expect2 := shardTree{
		shard: nilParent,
		children: []*shardTree{
			{
				shard:    child,
				children: nil,
			},
		},
	}

	require.Equal(t, []*shardTree{&expect1, &expect2}, buildShardTrees(shards))
}
