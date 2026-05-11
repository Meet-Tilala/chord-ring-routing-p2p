# Chord & Ring Routing — Distributed Task Processing over P2P Overlays

> Discrete-event simulation comparing **O(N) ring routing** and **O(log N) Chord finger-table routing** for distributed task decomposition, result aggregation, and gossip-based consensus, built with **OMNeT++ 6** and **C++17**.

---

## Overview

This project implements and compares two peer-to-peer routing strategies for **distributed computation** and **gossip-based consensus** within an overlay network:

| Variant | Routing Complexity | Strategy |
|---|---|---|
| **Ring Topology** | `O(N)` | Sequential hop-by-hop forwarding around a unidirectional ring |
| **Chord Protocol** | `O(log N)` | Finger-table-accelerated routing with logarithmic hop distances |

Each client in the network autonomously:
1. **Generates** a random array of `K` integers.
2. **Decomposes** the computation into `X` subtasks, each assigned to peer `i % N`.
3. **Routes** subtask messages to the responsible peer using the overlay topology.
4. **Computes** the partial maximum of received data chunks.
5. **Aggregates** all partial maxima into a final global result.
6. **Broadcasts** a timestamped gossip message upon task completion.
7. **Terminates** only after every node in the network has been observed via gossip.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      OMNeT++ Simulation                     │
│                                                             │
│   ┌──────────┐   TaskMsg    ┌──────────┐   TaskMsg          │
│   │ Client 0 │ ──────────▶ │ Client 1 │ ──────────▶ ...    │
│   │          │ ◀────────── │          │ ◀──────────        │
│   └──────────┘  ResultMsg   └──────────┘  ResultMsg         │
│        │                         │                          │
│        │       GossipMsg         │       GossipMsg          │
│        └────────────────────────▶│◀─────────────────────    │
│                                                             │
│   Topology loaded from config.txt at runtime                │
│   Finger tables built dynamically (Chord variant only)      │
└─────────────────────────────────────────────────────────────┘
```

### Message Types

| Message | Fields | Purpose |
|---|---|---|
| `TaskMessage` | `taskOwnerId`, `subtaskId`, `destinationClientId`, `hopCount`, `data[]` | Carries a data chunk to the assigned processor |
| `ResultMessage` | `taskOwnerId`, `subtaskId`, `processorClientId`, `hopCount`, `result` | Returns the computed partial maximum to the task owner |
| `GossipMessage` | `content`, `originIp`, `originClientId`, `messageHash` | Flood-based dissemination for consensus termination |

---

##  Project Structure

```
.
├── Normal_ringTopology_On/         # O(N) Ring Routing Implementation
│   ├── Client.h / Client.cc       # RingClient module (topology, BFS routing, gossip)
│   ├── Client.ned                 # NED module declaration
│   ├── Network.ned                # RingNetwork compound module
│   ├── messages.msg               # Ring message definitions
│   ├── omnetpp.ini                # Simulation configurations (Small / Medium / Large)
│   ├── config.txt                 # 8-node ring topology
│   ├── config_16.txt              # 16-node ring topology
│   ├── config_32.txt              # 32-node ring topology
│   └── outputfile*.txt            # Sample simulation output
│
├── Chord_Protocol_Ologn/           # O(log N) Chord Routing Implementation
│   ├── Client.h / Client.cc       # ChordClient module (finger table, greedy routing, gossip)
│   ├── ChordNetwork.ned           # NED network + module declaration
│   ├── ChordMessages.msg          # Chord message definitions
│   ├── omnetpp.ini                # Simulation configurations (Small / Medium / Large)
│   ├── config.txt                 # 8-node Chord topology with finger entries
│   ├── config_16.txt              # 16-node Chord topology
│   ├── config_32.txt              # 32-node Chord topology
│   └── outputfile*.txt            # Sample simulation output
│
└── README.md
```

---

##  Prerequisites

- [**OMNeT++ 6.x**](https://omnetpp.org/download/) — discrete-event simulation framework
- A **C++17** compatible compiler (GCC ≥ 7, Clang ≥ 5, or MSVC ≥ 19.14)
- GNU Make

---

##  Build & Run

### Ring Topology — `O(N)` Routing

```bash
cd Normal_ringTopology_On
opp_makemake -f
make
opp_run -u Cmdenv omnetpp.ini -c Small
```

### Chord Protocol — `O(log N)` Routing

```bash
cd Chord_Protocol_Ologn
opp_makemake -f
make
opp_run -u Cmdenv omnetpp.ini -c Small
```

### Available Configurations

| Config | Nodes (`N`) | Array Size (`K`) | Subtasks (`X`) |
|---|---|---|---|
| `Small` | 8 | 64 | 16 |
| `Medium` | 16 | 128 | 32 |
| `Large` | 32 | 256 | 64 |

> Replace `-c Small` with `-c Medium` or `-c Large` to switch configurations.

---

##  How It Works

### 1. Task Decomposition & Distribution

Each client generates a random integer array of size `K` and splits it into `X` subtasks. Subtask `i` is deterministically mapped to client `i % N`. If `K / X < 2`, the subtask count is automatically adjusted downward to maintain a minimum chunk size of 2.

### 2. Routing Strategies

#### Ring Routing (`O(N)`)

Messages traverse the ring sequentially. The next-hop is determined via **BFS** over an adjacency map built from the topology file. Worst-case path length equals `N - 1` hops.

#### Chord Routing (`O(log N)`)

Each client constructs a **finger table** with entries pointing to nodes at exponentially increasing distances:

```
finger[k] = (clientId + 2^k) mod N    for k = 0, 1, ..., ⌈log₂N⌉ - 1
```

Greedy forwarding selects the neighbor that makes the **greatest clockwise progress** toward the destination without overshooting. This reduces the expected hop count to `O(log N)`.

### 3. Gossip-Based Consensus

Once a client finishes aggregating its task results, it broadcasts a gossip message containing `<timestamp>:<IP>:<clientId>`. Gossip uses **flood-fill**: each node forwards a first-seen message to all neighbors except the sender. A node terminates only after collecting gossip from **all `N` clients**, ensuring global consensus on task completion.

---

##  Sample Output

Each simulation run produces:

| File | Description |
|---|---|
| `outputfile.txt` | Unified log from all clients (prefixed with `[Client X]`) |
| `outputfile_client<ID>.txt` | Per-client log with task, result, and gossip events |

**Example log entries:**

```
Client 0 initialized with 4 CHORD neighbors.
Client 0 finger table: 1 2 4
Client 0 starting task with 16 subtasks.
Dispatching subtask 1 from client 0 to client 1 via CHORD neighbor 1.
Client 0 processed subtask 0 for task owner 0 and computed max = 997 after 0 hop(s).
Client 0 consolidated final result for its task: 1000.
Client 0 generated gossip 0.1:10.0.0.1:0.
Client 0 has received gossip from all 8 clients.
```

---

##  Customization

### Editing Topologies

Topology files use a simple adjacency-list format:

```
# Format: ClientID: neighbor1, neighbor2, ...
0: 1, 7
1: 2, 0
2: 3, 1
```

Modify `config.txt`, `config_16.txt`, or `config_32.txt` to experiment with different overlay structures. For Chord, the configuration includes finger-table shortcuts alongside ring predecessors.

### Custom Simulation Parameters

Use the `Custom` config to sweep parameter combinations:

```bash
opp_run -u Cmdenv omnetpp.ini -c Custom
```

This runs all valid `(N, K, X)` combinations subject to the constraint `X > N && K / X >= 2`.

---

##  Tech Stack

| Component | Technology |
|---|---|
| Simulation Framework | OMNeT++ 6 |
| Implementation Language | C++17 |
| Message Definitions | OMNeT++ `.msg` files (auto-generates serialization code) |
| Network Modeling | NED (Network Description Language) |
| Build System | `opp_makemake` + GNU Make |

