#!/usr/bin/env python3
"""
Distributed Transaction Coordinator
Supports: 2PC, 3PC, failure scenarios, timeouts, abort handling
"""

import json
import time
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib import request, error
from urllib.parse import urlparse, parse_qs
import sys
import threading

class TransactionCoordinator:
    def __init__(self, node_id, participants):
        self.node_id = node_id
        self.participants = participants
        self.transactions = {}
        self.lock = threading.Lock()
    
    def log(self, msg):
        print(f"[{self.node_id}] {msg}", flush=True)
    
    def send_message(self, url, endpoint, payload, timeout=5):
        """Send HTTP POST to participant"""
        try:
            full_url = f"{url}{endpoint}"
            req = request.Request(
                full_url,
                data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            with request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except error.URLError as e:
            self.log(f"Failed to contact {url}: {e}")
            return None
        except Exception as e:
            self.log(f"Error sending to {url}: {e}")
            return None
    
    def run_2pc(self, txid, operation, simulate_crash=False, simulate_timeout=False):
        """Two-Phase Commit Protocol"""
        self.log(f"Starting 2PC for {txid}")
        
        with self.lock:
            self.transactions[txid] = {
                'txid': txid,
                'protocol': '2PC',
                'state': 'INIT',
                'op': operation,
                'votes': {},
                'decision': None,
                'participants': self.participants.copy(),
                'ts': time.time()
            }
        
        # PHASE 1: PREPARE
        self.log(f"TX {txid} PREPARE phase")
        votes = {}
        
        for participant_url in self.participants:
            if simulate_timeout and participant_url == self.participants[0]:
                self.log(f"*** SIMULATING TIMEOUT from {participant_url} ***")
                time.sleep(6)  # Exceed timeout
                votes[participant_url] = 'TIMEOUT'
                continue
            
            resp = self.send_message(participant_url, '/prepare', {
                'txid': txid,
                'operation': operation
            })
            
            if resp and resp.get('vote'):
                vote = resp['vote']
                votes[participant_url] = vote
                self.log(f"TX {txid} received vote {vote} from {participant_url}")
            else:
                votes[participant_url] = 'NO'
                self.log(f"TX {txid} received NO vote (timeout/error) from {participant_url}")
        
        # DECISION
        if all(v == 'YES' for v in votes.values()):
            decision = 'COMMIT'
        else:
            decision = 'ABORT'
        
        self.log(f"TX {txid} decision: {decision}")
        
        with self.lock:
            self.transactions[txid]['votes'] = votes
            self.transactions[txid]['decision'] = decision
        
        # Simulate crash before sending decision
        if simulate_crash:
            self.log(f"*** SIMULATING COORDINATOR CRASH ***")
            return {'decision': decision, 'votes': votes, 'crashed': True}
        
        # PHASE 2: COMMIT or ABORT
        for participant_url in self.participants:
            if votes.get(participant_url) == 'TIMEOUT':
                continue  # Skip participants that timed out
            
            endpoint = '/commit' if decision == 'COMMIT' else '/abort'
            resp = self.send_message(participant_url, endpoint, {'txid': txid})
            
            if resp:
                self.log(f"TX {txid} sent {decision} to {participant_url}")
        
        with self.lock:
            self.transactions[txid]['state'] = 'DONE'
        
        return {'decision': decision, 'votes': votes}
    
    def run_3pc(self, txid, operation):
        """Three-Phase Commit Protocol"""
        self.log(f"Starting 3PC for {txid}")
        
        with self.lock:
            self.transactions[txid] = {
                'txid': txid,
                'protocol': '3PC',
                'state': 'INIT',
                'op': operation,
                'votes': {},
                'decision': None,
                'participants': self.participants.copy(),
                'ts': time.time()
            }
        
        # PHASE 1: CAN-COMMIT
        self.log(f"TX {txid} CAN-COMMIT phase")
        votes = {}
        
        for participant_url in self.participants:
            resp = self.send_message(participant_url, '/can_commit', {
                'txid': txid,
                'operation': operation
            })
            
            if resp and resp.get('vote'):
                vote = resp['vote']
                votes[participant_url] = vote
                self.log(f"TX {txid} CAN-COMMIT received {vote} from {participant_url}")
            else:
                votes[participant_url] = 'NO'
                self.log(f"TX {txid} CAN-COMMIT received NO from {participant_url}")
        
        # Check if we should proceed
        if not all(v == 'YES' for v in votes.values()):
            decision = 'ABORT'
            self.log(f"TX {txid} decision: {decision} (not all YES in CAN-COMMIT)")
            
            # Send ABORT
            for participant_url in self.participants:
                self.send_message(participant_url, '/abort', {'txid': txid})
                self.log(f"TX {txid} sent ABORT to {participant_url}")
            
            with self.lock:
                self.transactions[txid]['votes'] = votes
                self.transactions[txid]['decision'] = decision
                self.transactions[txid]['state'] = 'DONE'
            
            return {'decision': decision, 'votes': votes}
        
        # PHASE 2: PRE-COMMIT
        self.log(f"TX {txid} PRE-COMMIT phase")
        pre_commit_acks = {}
        
        for participant_url in self.participants:
            resp = self.send_message(participant_url, '/pre_commit', {'txid': txid})
            
            if resp and resp.get('ack'):
                pre_commit_acks[participant_url] = 'ACK'
                self.log(f"TX {txid} PRE-COMMIT ACK from {participant_url}")
            else:
                pre_commit_acks[participant_url] = 'NACK'
                self.log(f"TX {txid} PRE-COMMIT NACK from {participant_url}")
        
        # If not all ACKs, abort
        if not all(a == 'ACK' for a in pre_commit_acks.values()):
            decision = 'ABORT'
            self.log(f"TX {txid} decision: {decision} (not all ACK in PRE-COMMIT)")
            
            for participant_url in self.participants:
                self.send_message(participant_url, '/abort', {'txid': txid})
            
            with self.lock:
                self.transactions[txid]['votes'] = votes
                self.transactions[txid]['decision'] = decision
                self.transactions[txid]['state'] = 'DONE'
            
            return {'decision': decision, 'votes': votes}
        
        # PHASE 3: DO-COMMIT
        self.log(f"TX {txid} DO-COMMIT phase")
        decision = 'COMMIT'
        
        for participant_url in self.participants:
            resp = self.send_message(participant_url, '/do_commit', {'txid': txid})
            
            if resp:
                self.log(f"TX {txid} DO-COMMIT sent to {participant_url}")
        
        with self.lock:
            self.transactions[txid]['votes'] = votes
            self.transactions[txid]['decision'] = decision
            self.transactions[txid]['state'] = 'DONE'
        
        return {'decision': decision, 'votes': votes}


class CoordinatorHTTPHandler(BaseHTTPRequestHandler):
    coordinator = None
    
    def log_message(self, format, *args):
        pass  # Suppress default HTTP logs
    
    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            status = {
                'ok': True,
                'node': self.coordinator.node_id,
                'port': self.server.server_port,
                'participants': self.coordinator.participants,
                'tx': self.coordinator.transactions
            }
            self.wfile.write(json.dumps(status, indent=2).encode('utf-8'))
        else:
            self.send_error(404)
    
    def do_POST(self):
        if self.path == '/tx/start':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            payload = json.loads(body)
            
            txid = payload['txid']
            protocol = payload.get('protocol', '2PC')
            operation = payload['operation']
            simulate_crash = payload.get('simulate_crash', False)
            simulate_timeout = payload.get('simulate_timeout', False)
            
            if protocol == '2PC':
                result = self.coordinator.run_2pc(txid, operation, simulate_crash, simulate_timeout)
            elif protocol == '3PC':
                result = self.coordinator.run_3pc(txid, operation)
            else:
                self.send_error(400, f"Unknown protocol: {protocol}")
                return
            
            if result.get('crashed'):
                return
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            response = {
                'ok': True,
                'txid': txid,
                'protocol': protocol,
                'decision': result['decision'],
                'votes': result['votes']
            }
            self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))
        else:
            self.send_error(404)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True, help='Coordinator ID')
    parser.add_argument('--port', type=int, required=True, help='Port to listen on')
    parser.add_argument('--participants', required=True, help='Comma-separated participant URLs')
    args = parser.parse_args()
    
    participants = [p.strip() for p in args.participants.split(',')]
    
    coordinator = TransactionCoordinator(args.id, participants)
    CoordinatorHTTPHandler.coordinator = coordinator
    
    server = HTTPServer(('0.0.0.0', args.port), CoordinatorHTTPHandler)
    coordinator.log(f"Coordinator listening on 0.0.0.0:{args.port} participants={participants}")
    
    server.serve_forever()


if __name__ == '__main__':
    main()
