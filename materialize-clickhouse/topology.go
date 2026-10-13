package connector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// replica is one member of a ClickHouse cluster as system.clusters lists it.
type replica struct {
	Shard   uint32
	Replica uint32
	Weight  uint32
	Host    string
	Port    uint16
	Errors  uint32
}

func (r replica) address() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// topology is a cluster's shard layout, fixed for the life of a session so
// that every row of every transaction is placed under one view of the
// cluster.
type topology struct {
	Cluster  string
	Replicas []replica // sorted by shard, then replica
	ReadAt   time.Time
}

// readTopology reads the cluster's layout from the server the connection is
// open to.
func readTopology(ctx context.Context, conn chdriver.Conn, cluster string) (*topology, error) {
	rows, err := conn.Query(ctx, `
		SELECT shard_num, replica_num, shard_weight, host_address, port, errors_count
		FROM system.clusters WHERE cluster = ?
		ORDER BY shard_num, replica_num`, cluster)
	if err != nil {
		return nil, fmt.Errorf("querying system.clusters for %q: %w", cluster, err)
	}
	defer rows.Close()

	var topo = &topology{Cluster: cluster, ReadAt: time.Now()}
	for rows.Next() {
		var r replica
		if err := rows.Scan(&r.Shard, &r.Replica, &r.Weight, &r.Host, &r.Port, &r.Errors); err != nil {
			return nil, fmt.Errorf("scanning system.clusters: %w", err)
		}
		topo.Replicas = append(topo.Replicas, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading system.clusters: %w", err)
	}
	if len(topo.Replicas) == 0 {
		return nil, fmt.Errorf("cluster %q is not defined on this server", cluster)
	}
	return topo, nil
}

// shardCount is the number of shards, which system.clusters numbers from 1
// without gaps.
func (t *topology) shardCount() int {
	return int(t.Replicas[len(t.Replicas)-1].Shard)
}

// weights returns each shard's weight, indexed by shard number minus one.
func (t *topology) weights() []uint32 {
	var out = make([]uint32, t.shardCount())
	for _, r := range t.Replicas {
		out[r.Shard-1] = r.Weight
	}
	return out
}

// pickReplicas chooses one replica per shard to hold this session's
// connections, preferring one the initiating server has not seen errors
// from.
func (t *topology) pickReplicas() []replica {
	var byShard = make([][]replica, t.shardCount())
	for _, r := range t.Replicas {
		byShard[r.Shard-1] = append(byShard[r.Shard-1], r)
	}
	var out = make([]replica, 0, len(byShard))
	for _, candidates := range byShard {
		var pick = candidates[0]
		for _, c := range candidates {
			if c.Errors == 0 {
				pick = c
				break
			}
		}
		out = append(out, pick)
	}
	return out
}

// equal reports whether two layouts place rows identically: the same shards,
// with the same weights, made of the same members. Error counts are
// transient and do not affect placement.
func (t *topology) equal(o *topology) bool {
	if t.Cluster != o.Cluster || len(t.Replicas) != len(o.Replicas) {
		return false
	}
	for i := range t.Replicas {
		var a, b = t.Replicas[i], o.Replicas[i]
		if a.Shard != b.Shard || a.Replica != b.Replica || a.Weight != b.Weight || a.Host != b.Host || a.Port != b.Port {
			return false
		}
	}
	return true
}

func (t *topology) logFields() log.Fields {
	var members = make([]string, 0, len(t.Replicas))
	for _, r := range t.Replicas {
		members = append(members, fmt.Sprintf("s%d/r%d@%s w%d", r.Shard, r.Replica, r.address(), r.Weight))
	}
	return log.Fields{"cluster": t.Cluster, "shards": t.shardCount(), "members": members}
}

// shardConns holds a session's connections to the replica chosen for one
// shard. Load and store get separate pools, as on a single server, because
// the two phases of a transaction overlap.
type shardConns struct {
	replica replica
	load    chdriver.Conn
	store   chdriver.Conn
}

func (s *shardConns) close() {
	_ = s.load.Close()
	_ = s.store.Close()
}

// quorumSettings makes every store insert wait for a majority of the shard's
// replicas, so that a committed part survives the loss of the replica it was
// written to and so that select_sequential_consistency reads can see it.
func quorumSettings(cfg config) clickhouse.Settings {
	return clickhouse.Settings{
		"insert_quorum":          "auto",
		"insert_quorum_parallel": 0,
		"insert_quorum_timeout":  cfg.insertQuorumTimeout().Milliseconds(),
	}
}

// openShardConns opens connections to one replica per shard and proves that
// each connection reached the shard it was meant for, so that a transposed
// or stale address fails at startup instead of misplacing rows.
func openShardConns(ctx context.Context, cfg config, topo *topology) ([]*shardConns, error) {
	var out []*shardConns
	var closeAll = func() {
		for _, s := range out {
			s.close()
		}
	}

	for _, r := range topo.pickReplicas() {
		var nodeCfg = cfg
		nodeCfg.Address = r.address()

		load, err := clickhouse.Open(nodeCfg.newClickhouseOptions())
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("opening load connection to shard %d at %s: %w", r.Shard, r.address(), err)
		}
		var storeOpts = nodeCfg.newClickhouseOptions()
		storeOpts.Settings = quorumSettings(cfg)
		store, err := clickhouse.Open(storeOpts)
		if err != nil {
			load.Close()
			closeAll()
			return nil, fmt.Errorf("opening store connection to shard %d at %s: %w", r.Shard, r.address(), err)
		}
		var s = &shardConns{replica: r, load: load, store: store}
		out = append(out, s)

		for _, conn := range []chdriver.Conn{load, store} {
			if err := assertShard(ctx, conn, topo.Cluster, r); err != nil {
				closeAll()
				return nil, err
			}
		}
	}
	return out, nil
}

// assertShard checks that the server at the other end of conn reports itself
// as the given member of the cluster.
func assertShard(ctx context.Context, conn chdriver.Conn, cluster string, want replica) error {
	rows, err := conn.Query(ctx, `
		SELECT shard_num, replica_num FROM system.clusters
		WHERE cluster = ? AND is_local`, cluster)
	if err != nil {
		return fmt.Errorf("querying local membership of %s in cluster %q: %w", want.address(), cluster, err)
	}
	defer rows.Close()

	var local []string
	for rows.Next() {
		var shard, rep uint32
		if err := rows.Scan(&shard, &rep); err != nil {
			return fmt.Errorf("scanning local membership: %w", err)
		}
		if shard == want.Shard && rep == want.Replica {
			return nil
		}
		local = append(local, "s"+strconv.Itoa(int(shard))+"/r"+strconv.Itoa(int(rep)))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading local membership: %w", err)
	}
	return fmt.Errorf("server at %s does not identify as shard %d replica %d of cluster %q (it reports %v); check the cluster's host addresses are reachable under the same names from Estuary",
		want.address(), want.Shard, want.Replica, cluster, local)
}

// refreshTopology re-reads the cluster layout once the session's view is
// older than the configured interval, and fails the session when the layout
// has changed so that the next session starts from the new one. Staged data
// is left in place for that session to recover.
func (t *transactor) refreshTopology(ctx context.Context) error {
	if t.topology == nil || time.Since(t.topology.ReadAt) < t.cfg.topologyRefreshInterval() {
		return nil
	}
	fresh, err := readTopology(ctx, t.store.conn, t.cfg.Cluster)
	if err != nil {
		return fmt.Errorf("refreshing cluster topology: %w", err)
	}
	if t.topology.equal(fresh) {
		t.topology.ReadAt = fresh.ReadAt
		return nil
	}
	log.WithFields(log.Fields{
		"eventType": "connectorStatus",
		"was":       t.topology.logFields(),
		"now":       fresh.logFields(),
	}).Warn("cluster topology changed; restarting to adopt the new layout")
	return fmt.Errorf("cluster %q topology changed from %d to %d shards (weights %v to %v); restarting to adopt the new layout",
		t.cfg.Cluster, t.topology.shardCount(), fresh.shardCount(), t.topology.weights(), fresh.weights())
}
