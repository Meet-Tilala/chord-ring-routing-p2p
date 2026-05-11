# CSL3080 Assignment 2 - CHORD `O(log N)` Variant

This directory contains the optimized peer-to-peer implementation that augments
the ring with CHORD-style shortcut links from the configuration file.

## Behavior

- Every client generates its own random array of `K` integers.
- Each client divides its task into `X` subtasks while preserving `K / X >= 2`.
- Subtask `i` is assigned to client `i % N`.
- Task and result routing use the shortcut links from `topology*.txt`, choosing
  the neighbor that makes the best clockwise progress toward the destination.
- Each origin client aggregates all partial maxima into its final result.
- After finishing its own task, each client emits one gossip message in the
  format `<timestamp>:<IP>:<ClientID>`.
- Nodes flood each first-seen gossip over the configured neighbors and stop
  only after every client's gossip has been observed.

## Build

```bash
cd Chord_Protocol_Ologn
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
