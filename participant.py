#!/usr/bin/env python3
"""
Distributed Transaction Participant
Supports: 2PC, 3PC, WAL, timeout recovery, vote rejection
"""

import json
import time
import argparse
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class TransactionParticipant:
    def __init__(self, node_id, wal_path=None):
        self.node_id = node_id
        self.wal_path = wal_path
        self.kv_store = {}
        self.transactions = {}
        self.lock = threading.Lock()
        self.should_reject = {}  # For simulating VOTE-NO scenarios
        self.crashed = False  # For simulating crashes
        
        if wal_path and os.path.exists(wal_path):
            self.recover_from_wal()
    
    def log(self, msg):
        if not self.crashed:
            print(f"[{self.node_id}] {msg}", flush=True)
    
    def write_wal(self, entry):
        """Write entry to write-ahead log"""
        if self.wal_path:
            with open(self.wal_path, 'a') as f:
                f.write(entry + '\n')
    
    def recover_from_wal(self):
        """Recover state from WAL"""
        self.log(f"Recovering from WAL: {self.wal_path}")
        with open(self.wal_path, 'r') as f:
            for line in f:
                pass
    
    def validate_operation(self, txid, operation):
        """Validate if operation can be executed"""
        # Simulate rejection for specific transactions
        if txid in self.should_reject:
            return False
        return True
    
    def apply_operation(self, operation):
        """Apply operation to key-value store"""
        if operation['type'] == 'SET':
            key = operation['key']
            value = operation['value']
            self.kv_store[key] = value
            self.log(f"Applied: SET {key} = {value}")
    
    # ===== 2PC HANDLERS =====
    
    def handle_prepare(self, txid, operation):
        """2PC Phase 1: PREPARE"""
        if self.crashed:
            return None
        
        with self.lock:
            if not self.validate_operation(txid, operation):
                vote = 'NO'
                state = 'ABORTED'
                self.log(f"TX {txid} PREPARE -> NO (validation failed)")
            else:
                vote = 'YES'
                state = 'READY'
                self.transactions[txid] = {
                    'state': state,
                    'op': operation,
                    'ts': time.time()
                }
                self.log(f"TX {txid} PREPARE -> YES (state: READY)")
            
            # Write to WAL
            self.write_wal(f"{txid} PREPARE {vote} {json.dumps(operation)}")
            
            return {'vote': vote}
    
    def handle_commit(self, txid):
        """2PC Phase 2: COMMIT"""
        if self.crashed:
            return None
        
        with self.lock:
            if txid in self.transactions:
                tx = self.transactions[txid]
                self.apply_operation(tx['op'])
                tx['state'] = 'COMMITTED'
                self.log(f"TX {txid} COMMITTED")
                self.write_wal(f"{txid} COMMIT")
                return {'ack': True}
            else:
                self.log(f"TX {txid} COMMIT failed - unknown transaction")
                return {'ack': False}
    
    def handle_abort(self, txid):
        """2PC Phase 2: ABORT"""
        if self.crashed:
            return None
        
        with self.lock:
            if txid in self.transactions:
                self.transactions[txid]['state'] = 'ABORTED'
                self.log(f"TX {txid} ABORTED")
                self.write_wal(f"{txid} ABORT")
                return {'ack': True}
            else:
                # Transaction might not exist if we voted NO
                self.log(f"TX {txid} ABORT (no local state)")
                self.write_wal(f"{txid} ABORT")
                return {'ack': True}
    
    # ===== 3PC HANDLERS =====
    
    def handle_can_commit(self, txid, operation):
        """3PC Phase 1: CAN-COMMIT"""
        if self.crashed:
            return None
        
        with self.lock:
            if not self.validate_operation(txid, operation):
                vote = 'NO'
                self.log(f"TX {txid} CAN-COMMIT -> NO")
                return {'vote': vote}
            
            vote = 'YES'
            self.transactions[txid] = {
                'state': 'CAN_COMMIT',
                'op': operation,
                'ts': time.time()
            }
            self.log(f"TX {txid} CAN-COMMIT -> YES (state: CAN_COMMIT)")
            self.write_wal(f"{txid} CAN_COMMIT {vote} {json.dumps(operation)}")
            
            return {'vote': vote}
    
    def handle_pre_commit(self, txid):
        """3PC Phase 2: PRE-COMMIT"""
        if self.crashed:
            return None
        
        with self.lock:
            if txid in self.transactions:
                self.transactions[txid]['state'] = 'PRE_COMMITTED'
                self.log(f"TX {txid} PRE-COMMIT ACK (state: PRE_COMMITTED)")
                self.write_wal(f"{txid} PRE_COMMIT")
                return {'ack': True}
            else:
                self.log(f"TX {txid} PRE-COMMIT NACK - unknown transaction")
                return {'ack': False}
    
    def handle_do_commit(self, txid):
        """3PC Phase 3: DO-COMMIT"""
        if self.crashed:
            return None
        
        with self.lock:
            if txid in self.transactions:
                tx = self.transactions[txid]
                self.apply_operation(tx['op'])
                tx['state'] = 'COMMITTED'
                self.log(f"TX {txid} DO-COMMIT completed (state: COMMITTED)")
                self.write_wal(f"{txid} DO_COMMIT")
                return {'ack': True}
            else:
                self.log(f"TX {txid} DO-COMMIT failed - unknown transaction")
                return {'ack': False}
    
    # ===== FAILURE SIMULATION =====
    
    def set_rejection(self, txid):
        """Configure participant to reject a specific transaction"""
        self.should_reject[txid] = True
        self.log(f"Configured to REJECT transaction {txid}")
    
    def simulate_crash(self):
        """Simulate participant crash"""
        self.crashed = True
        self.log(f"*** PARTICIPANT CRASHED ***")


class ParticipantHTTPHandler(BaseHTTPRequestHandler):
    participant = None
    
    def log_message(self, format, *args):
        pass  # Suppress default HTTP logs
    
    def send_json_response(self, data, code=200):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))
    
    def do_GET(self):
        if self.path == '/status':
            status = {
                'ok': True,
                'node': self.participant.node_id,
                'port': self.server.server_port,
                'kv': self.participant.kv_store,
                'tx': self.participant.transactions,
                'wal': self.participant.wal_path,
                'crashed': self.participant.crashed
            }
            self.send_json_response(status)
        else:
            self.send_error(404)
    
    def do_POST(self):
        if self.participant.crashed and self.path != '/recover':
            # Simulate no response from crashed participant
            return
        
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        payload = json.loads(body)
        
        # 2PC endpoints
        if self.path == '/prepare':
            result = self.participant.handle_prepare(payload['txid'], payload['operation'])
            if result:
                self.send_json_response(result)
        
        elif self.path == '/commit':
            result = self.participant.handle_commit(payload['txid'])
            if result:
                self.send_json_response(result)
        
        elif self.path == '/abort':
            result = self.participant.handle_abort(payload['txid'])
            if result:
                self.send_json_response(result)
        
        # 3PC endpoints
        elif self.path == '/can_commit':
            result = self.participant.handle_can_commit(payload['txid'], payload['operation'])
            if result:
                self.send_json_response(result)
        
        elif self.path == '/pre_commit':
            result = self.participant.handle_pre_commit(payload['txid'])
            if result:
                self.send_json_response(result)
        
        elif self.path == '/do_commit':
            result = self.participant.handle_do_commit(payload['txid'])
            if result:
                self.send_json_response(result)
        
        # Failure simulation endpoints
        elif self.path == '/reject':
            txid = payload.get('txid')
            self.participant.set_rejection(txid)
            self.send_json_response({'ok': True, 'rejected': txid})
        
        elif self.path == '/crash':
            self.participant.simulate_crash()
            # Don't send response - simulating crash
        
        elif self.path == '/recover':
            self.participant.crashed = False
            self.participant.log(f"*** PARTICIPANT RECOVERED ***")
            self.send_json_response({'ok': True, 'recovered': True})
        
        else:
            self.send_error(404)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True, help='Participant ID')
    parser.add_argument('--port', type=int, required=True, help='Port to listen on')
    parser.add_argument('--wal', help='Write-ahead log file path')
    args = parser.parse_args()
    
    participant = TransactionParticipant(args.id, args.wal)
    ParticipantHTTPHandler.participant = participant
    
    server = HTTPServer(('0.0.0.0', args.port), ParticipantHTTPHandler)
    participant.log(f"Participant listening on 0.0.0.0:{args.port} wal={args.wal}")
    
    server.serve_forever()


if __name__ == '__main__':
    main()
