# CSL3080 Assignment 2 - Ring Topology `O(N)` Variant

This directory contains the baseline peer-to-peer implementation where clients
communicate only over the editable ring topology from the configuration file.

## Behavior

- Every client generates its own random array of `K` integers.
- Each client divides its task into `X` subtasks while preserving `K / X >= 2`.
- Subtask `i` is assigned to client `i % N`.
- Routing happens over the ring, so message exchange complexity is `O(N)` in
  the worst case.
- Each origin client aggregates all partial maxima into its final result.
- After finishing its own task, each client emits one gossip message in the
  format `<timestamp>:<IP>:<ClientID>`.
- Nodes forward each first-seen gossip once and terminate only after every
  client's gossip has been observed.

## Build

```bash
cd Normal_ringTopology_On
opp_makemake -f
make
```

## Run

```bash
opp_run -u Cmdenv omnetpp.ini -c Small
```

Available configurations:

- `Small`: `N=8`, `K=64`, `X=16`
- `Medium`: `N=16`, `K=128`, `X=32`
- `Large`: `N=32`, `K=256`, `X=64`

Editable configuration files:

- `config.txt`
- `config_16.txt`
- `config_32.txt`

Output is written to the assignment-style shared file `outputfile.txt`, to
`outputfile_client<ID>.txt` for each client, and to the OMNeT++ console/event
log.
