# Lab 4 - Distributed Transactions (2PC/3PC)

Implementation of Two-Phase Commit and Three-Phase Commit protocols on AWS EC2 cluster.

## Files

- `coordinator.py` - Transaction coordinator (2PC/3PC)
- `participant.py` - Transaction participant (2PC/3PC)
- `client.py` - Client for testing

## Deployment

### Start Coordinator (Terminal 1)

```bash
python3 coordinator.py --id COORD --port 8000 \
  --participants http://172.31.20.152:8001,http://172.31.18.54:8002
```

### Start Participants (Terminal 2 & 3)

```bash
# Participant 1
python3 participant.py --id P1 --port 8001 --wal /tmp/participant_P1.wal

# Participant 2
python3 participant.py --id P2 --port 8002 --wal /tmp/participant_P2.wal
```

## Cluster Configuration

| Node | IP | Port |
|------|-----|------|
| Coordinator | 172.31.31.75 | 8000 |
| Participant 1 | 172.31.20.152 | 8001 |
| Participant 2 | 172.31.18.54 | 8002 |
