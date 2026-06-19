# InfraLens Phase 2 — Distributed System Foundation

## 1. Overview

Phase 2 turns the single-node storage engine from Phase 1 into a horizontally scalable
cluster capable of 10 M events/s ingestion across N nodes.  The key additions are:

- **Consistent hash ring** — routes writes and queries to the correct storage shard.
- **etcd control plane** — authoritative store for cluster membership, shard ownership, and leader election.
- **Async replication** — every write is replicated to `replication_factor − 1` peers via a WAL-shipping protocol before acknowledging the client.
- **Scatter/gather** — the query coordinator fans a query out across all shards that cover the requested time range and merges the Arrow IPC result streams.
- **Internal gRPC service** — all intra-cluster traffic (replication, scatter/gather) runs over a dedicated port (`:4319`).

---

## 2. Cluster Topology

```
                        ┌─────────────────────┐
                        │     etcd cluster     │
                        │  (3 or 5 members)    │
                        └──────────┬──────────┘
                                   │ membership / shard map
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
    ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
    │  Node A     │◄──────►│  Node B     │◄──────►│  Node C     │
    │  :4317/4318 │  WAL   │  :4317/4318 │  WAL   │  :4317/4318 │
    │  :4319 RPC  │ ship   │  :4319 RPC  │ ship   │  :4319 RPC  │
    └─────────────┘        └─────────────┘        └─────────────┘
           │                       │                       │
      shard 0,3,6             shard 1,4,7             shard 2,5,8
```

Each node owns a set of **primary shards** (partitions whose hash falls in the node's
ring segment) and holds **replica copies** of its neighbours' shards.

---

## 3. Consistent Hash Ring

- **Virtual nodes**: 150 per physical node (standard for uniform distribution).
- **Key function**: `shard_id = ring.locate(sha256(series_key)[..8])` where
  `series_key = "{signal}|{timestamp_bucket}|{sorted_labels}"`.
- **Ring persisted in etcd** so all nodes share a consistent view.
- **Rebalancing**: when a node joins/leaves, only the affected arc's data migrates.
  A background `RebalanceWorker` streams SSTables to the new primary.

---

## 4. Replication Protocol

Phase 2 uses **async primary-replica** replication (not Raft — deferred to Phase 6 if needed):

1. Primary writes to local WAL + MemTable.
2. Primary concurrently sends the WAL entry to `replication_factor − 1` replicas via a bidirectional gRPC stream.
3. After `min_replicas` acknowledge, the primary returns success to the client.
4. Replicas apply WAL entries in order; they maintain their own MemTable and SSTable files.

**Failure handling**: if a replica is unreachable, the primary buffers up to
`replica_buffer_bytes` of WAL entries. On reconnect, the replica requests a
catch-up stream starting from its last known sequence.

---

## 5. Scatter/Gather Query Execution

```
Client ──► API Gateway (Go, :8080)
               │
               ▼
        QueryCoordinator
               │
      ┌────────┴────────┐
      ▼                 ▼
  Node A shard      Node B shard
  ShardScan         ShardScan
      │                 │
      └────────┬────────┘
               ▼
         MergeSort
               │
               ▼
         Final result (Arrow IPC)
```

The coordinator:
1. Resolves which shards cover the query's time range via the ring + zone maps.
2. Sends `ShardQueryRequest` (serialised `PhysicalPlan`) to each shard node via gRPC.
3. Streams `RecordBatch`es back from each shard in parallel.
4. Performs a merge-sort + final aggregation on the coordinator.

---

## 6. Internal gRPC API

Defined in `proto/infralens/internal/v1/internal.proto`:

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `QueryShard` | coord → shard | Execute a partial plan on a shard; stream `RecordBatch`es back |
| `Replicate`  | primary → replica | Stream WAL entries for replication |
| `SyncShard`  | new node → primary | Full shard state transfer on join |
| `GetNodeInfo` | any → any | Health, shard list, ring version |

---

## 7. etcd Schema

| Key pattern | Value | Purpose |
|-------------|-------|---------|
| `/infralens/nodes/{node_id}` | `NodeMeta` JSON | Liveness (TTL lease) |
| `/infralens/ring/v{version}` | `RingConfig` JSON | Canonical ring snapshot |
| `/infralens/shards/{shard_id}/primary` | node_id | Who owns a shard |
| `/infralens/shards/{shard_id}/replicas` | `[node_id, …]` JSON | Replica set |
| `/infralens/leader` | node_id | Cluster-wide leader (for ring changes) |

---

## 8. New Crates

| Crate | Language | Responsibility |
|-------|----------|---------------|
| `infralens-cluster` | Rust | Ring, membership, shard routing, replication engine |
| `infralens-rpc` | Rust | Internal gRPC service impl (generated from `internal.proto`) |

Both crates are added to the workspace.  The server binary (`infralens-server`) is updated
to start the cluster subsystem and internal gRPC server when `cluster.enabled = true`.

---

## 9. Configuration additions (`default.toml`)

```toml
[cluster]
enabled           = false        # single-node mode by default
node_id           = ""           # auto-generated UUID if empty
internal_grpc_addr = "0.0.0.0:4319"
etcd_endpoints    = ["http://localhost:2379"]
replication_factor = 3
min_ack_replicas  = 1            # async: ack after local write
virtual_nodes     = 150
replica_buffer_bytes = 67108864  # 64 MiB WAL buffer per replica
```

---

## 10. Trade-offs vs. Phase 3+

| Decision | Phase 2 | Future |
|----------|---------|--------|
| Consensus | etcd leader election | Optional Raft per shard group |
| Replication | Async (possible data loss on crash) | Semi-sync or Raft log |
| Rebalance | Background SSTable migration | Online re-sharding without downtime |
| Cross-shard txn | None — events are independent | Two-phase commit for metadata |
