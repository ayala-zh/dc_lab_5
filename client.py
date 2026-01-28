#!/usr/bin/env python3
"""
Transaction Client
Supports: 2PC, 3PC, failure simulation, status checks
"""

import json
import sys
import argparse
from urllib import request, error

def post_json(url, payload, timeout=10):
    """Send POST request with JSON payload"""
    try:
        req = request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        with request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except error.HTTPError as e:
        return e.code, {'error': str(e)}
    except error.URLError as e:
        return 0, {'error': f'Connection failed: {e}'}
    except Exception as e:
        return 0, {'error': str(e)}

def get_json(url, timeout=10):
    """Send GET request"""
    try:
        with request.urlopen(url, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except error.HTTPError as e:
        return e.code, {'error': str(e)}
    except error.URLError as e:
        return 0, {'error': f'Connection failed: {e}'}
    except Exception as e:
        return 0, {'error': str(e)}

def main():
    parser = argparse.ArgumentParser(description='Transaction Client')
    parser.add_argument('--coord', required=True, help='Coordinator URL')
    parser.add_argument('command', help='Command: status, start, crash, reject, recover')
    parser.add_argument('args', nargs='*', help='Command arguments')
    args = parser.parse_args()
    
    base = args.coord.rstrip('/')
    
    if args.command == 'status':
        # Get coordinator status
        status, obj = get_json(base + '/status')
        print(status)
        print(json.dumps(obj, indent=2))
    
    elif args.command == 'start':
        if len(args.args) < 5:
            print("Usage: start TXID PROTOCOL OPERATION_TYPE KEY VALUE [--crash] [--timeout]")
            print("Example: start TX1 2PC SET x 10")
            print("Example: start TX2 3PC SET y 20 --crash")
            sys.exit(1)
        
        txid = args.args[0]
        protocol = args.args[1]
        op_type = args.args[2]
        key = args.args[3]
        value = args.args[4]
        
        # Check for flags
        simulate_crash = '--crash' in args.args
        simulate_timeout = '--timeout' in args.args
        
        operation = {
            'type': op_type,
            'key': key,
            'value': value
        }
        
        payload = {
            'txid': txid,
            'protocol': protocol,
            'operation': operation,
            'simulate_crash': simulate_crash,
            'simulate_timeout': simulate_timeout
        }
        
        status, obj = post_json(base + '/tx/start', payload, timeout=15)
        print(status)
        print(json.dumps(obj, indent=2))
    
    elif args.command == 'participant':
        # Get participant status
        if len(args.args) < 1:
            print("Usage: participant PARTICIPANT_URL")
            sys.exit(1)
        
        participant_url = args.args[0]
        status, obj = get_json(participant_url + '/status')
        print(status)
        print(json.dumps(obj, indent=2))
    
    elif args.command == 'reject':
        # Configure participant to reject a transaction
        if len(args.args) < 2:
            print("Usage: reject PARTICIPANT_URL TXID")
            sys.exit(1)
        
        participant_url = args.args[0]
        txid = args.args[1]
        
        status, obj = post_json(participant_url + '/reject', {'txid': txid})
        print(status)
        print(json.dumps(obj, indent=2))
    
    elif args.command == 'crash':
        # Simulate participant crash
        if len(args.args) < 1:
            print("Usage: crash PARTICIPANT_URL")
            sys.exit(1)
        
        participant_url = args.args[0]
        status, obj = post_json(participant_url + '/crash', {})
        print(f"Crash command sent to {participant_url}")
        print(f"Status: {status}")
    
    elif args.command == 'recover':
        # Recover crashed participant
        if len(args.args) < 1:
            print("Usage: recover PARTICIPANT_URL")
            sys.exit(1)
        
        participant_url = args.args[0]
        status, obj = post_json(participant_url + '/recover', {})
        print(status)
        print(json.dumps(obj, indent=2))
    
    else:
        print(f"Unknown command: {args.command}")
        print("Available commands: status, start, participant, reject, crash, recover")
        sys.exit(1)

if __name__ == '__main__':
    main()
