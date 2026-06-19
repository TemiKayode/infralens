# Cluster Architecture

**Crates:** `crates/infralens-cluster`, `crates/infralens-rpc`

InfraLens runs as a single process in development and as a distributed cluster in
production. This document covers the distributed mode: how nodes discover each other,
how data is assigned to nodes, and how queries are fanned out and merged.

---

## Overview

```
                        etcd (external)
                           │  watch /infralens/members/*
                ┌──────────┼──────────┐
                ▼          ▼          ▼
           node-1      node-2      node-3
             │            │            │
             └────────────┼────────────┘
                    consistent-hash ring
                    (virtual nodes per member)
```

Each InfraLens node:
1. Registers itself in etcd on startup.
2. Watches etcd for membership changes and rebuilds the hash ring.
3. Owns a contiguous arc of the hash ring and stores the corresponding data shards.
4. Serves scatter/gather gRPC on port 5317.

---

## Consistent-Hash Ring

**File:** `crates/infralens-cluster/src/ring.rs`

InfraLens uses a consistent-hash ring to assign time-partition shards to nodes. The
ring has 360 virtual node slots (configurable via `cluster.virtual_nodes`).

### Shard assignment

A partition key (e.g., `2024120114`) is hashed to a position on the ring. The node
that owns the nearest clockwise slot becomes the primary owner of that partition:

```
hash(partition_key) mod 360 → ring position
→ walk clockwise to nearest virtual node
→ that node owns this partition
```

With N real nodes, each node owns approximately `360 / N` virtual slots. Virtual nodes
(multiple slots per real node) smooth out imbalance when N is small.

### Rebalancing

When a node joins or leaves, only the partitions whose ring position maps to the
changed slots need to be moved. A node that acquires new slots pulls WAL segments from
the previous owner; a node that loses slots does nothing (the data remains on disk
and is not immediately deleted — archival is a future concern).

---

## Cluster Membership (etcd)

**File:** `crates/infralens-cluster/src/membership.rs`

Each node writes a lease-backed key to etcd on startup:

```
/infralens/members/<node_id>  →  {"addr": "10.0.0.1:5317", "joined_at": 1733059200}
```

The lease TTL is 15 seconds; the node heartbeats every 5 seconds. If a node crashes,
its key disappears after 15 seconds and all other nodes rebuild the ring without it.

Membership events are watched via the etcd watch API. Each change triggers:
1. `ClusterMembership::rebuild_ring()` — recomputes virtual node assignments.
2. `ReplicaSender::reconfigure()` — updates the set of replication targets.

---

## WAL Replication

**File:** `crates/infralens-cluster/src/replication.rs`

InfraLens uses asynchronous WAL replication (not Raft consensus). The trade-off:
lower write latency at the cost of potential data loss on the failure of the primary
node before replication completes.

### How it works

1. On every `WAL::append()` on the primary node, the WAL entry is also sent to replica
   nodes via a dedicated `ReplicaSender` per peer.
2. Each `ReplicaSender` owns an `mpsc::Sender<ReplicationEntry>` and a background Tokio
   task that drains the channel and forwards entries over a persistent gRPC stream.
3. Replicas apply the received entries to their own WAL and MemTable, becoming hot
   standbys for their partition range.
4. If the primary node fails, the ring is rebuilt and a replica is promoted by virtue
   of being the next clockwise owner for the affected slots.

### Replication factor

The default replication factor is 1 (primary only, no replicas). Set
`cluster.replication_factor = 3` for production to maintain 2 replicas.

### Consistency guarantee

Reads are served from the local node only (no quorum read). In a split-brain scenario
where two nodes both believe they own a slot, both will serve reads from their local
data — divergence is bounded by the replication lag at the time of the split.

---

## Scatter/Gather RPC

**File:** `crates/infralens-rpc/src/scatter_gather.rs`

The API Gateway connects to a single InfraLens node (configurable via `QUERY_BACKEND`).
That node acts as the query coordinator:

1. It receives the IQL query from the gateway.
2. It determines which nodes own which partitions that fall within the query's time range.
3. It fans the query out to all relevant nodes in parallel (`tokio::join_all`).
4. It collects `Vec<RecordBatch>` from each node and merge-sorts them by the query's
   `ORDER BY` key (or by `timestamp_ns` if no `ORDER BY` is specified).
5. It applies the global `LIMIT` after merging.
6. It streams the result back to the gateway as Arrow IPC.

```
Gateway ──gRPC──► Coordinator (node-1)
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
        node-1      node-2      node-3
        (local)   (gRPC call) (gRPC call)
          │           │           │
          └───────────┴───────────┘
                      │ merge-sort
                      ▼
                    Result
```

### Error handling

If a node is unreachable, the coordinator retries once with a 500 ms timeout. If the
retry also fails, the coordinator returns the partial result (from the reachable nodes)
with an `x-partial-result: true` HTTP header set on the gateway response. This is the
"best-effort" degradation mode — a future improvement could return an error instead.

---

## Internal gRPC Protocol

**File:** `proto/infralens/internal/v1/internal.proto`

The scatter/gather protocol defines two RPCs:

```protobuf
service InfraLensInternal {
  // Execute a query on this node's local data
  rpc LocalQuery(LocalQueryRequest) returns (stream QueryResultChunk);

  // Replicate a WAL entry from the primary
  rpc ReplicateWal(stream WalEntry) returns (ReplicateAck);
}
```

`LocalQueryRequest` carries the full serialised `LogicalPlan` (bincode-encoded) and
the list of partition directories to scan. The coordinator sends the already-optimised
plan so each node runs only the executor, not the full planner/optimizer stack.

---

## Single-Node Mode

When `cluster.etcd_endpoints` is empty, InfraLens starts in single-node mode:
- No etcd connection is made.
- The ring is initialised with a single virtual node covering all slots.
- Replication is disabled.
- The scatter/gather layer forwards queries directly to the local executor.

This is the default for `docker compose up` without multi-node profiles.

---

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `cluster.etcd_endpoints` | `""` | Comma-separated etcd URLs; empty = single-node mode |
| `cluster.node_id` | `"node-1"` | Unique node identifier |
| `cluster.replication_factor` | `1` | Number of copies per partition (1 = primary only) |
| `cluster.virtual_nodes` | `360` | Virtual slots on the hash ring |
| `cluster.lease_ttl_secs` | `15` | etcd lease TTL |
| `cluster.heartbeat_interval_secs` | `5` | etcd lease heartbeat |
