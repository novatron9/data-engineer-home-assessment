#!/usr/bin/env python3
"""
Task B: REST API Service for Timestamp Difference Calculator

Provides a REST endpoint that accepts plain text input with timestamps
and returns JSON array with calculated differences.
"""

from flask import Flask, request, jsonify
import sys
import os
import socket
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Task_A'))
from task_a import process_test_cases

app = Flask(__name__)

# Get node ID from environment or hostname
NODE_ID = os.environ.get('NODE_ID', socket.gethostname())


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'node_id': NODE_ID
    })


@app.route('/calculate', methods=['POST'])
def calculate_time_differences():
    """
    Calculate timestamp differences endpoint
    
    Expects plain text input with format:
    T (number of test cases)
    timestamp1
    timestamp2
    timestamp3
    timestamp4
    ...
    
    Returns JSON array with results
    """
    try:
        # Get plain text input
        input_text = request.get_data(as_text=True)
        
        if not input_text:
            return jsonify({
                'error': 'No input provided'
            }), 400
        
        # Process test cases using Task A logic
        results = process_test_cases(input_text)
        
        # Return as JSON array
        return jsonify(results)
        
    except Exception as e:
        return jsonify({
            'error': f'Error processing input: {str(e)}'
        }), 400


@app.route('/calculate-with-node', methods=['POST'])
def calculate_with_node_info():
    """
    Calculate timestamp differences with node information
    
    Returns JSON object with node ID and results array
    Used for Task C (multi-node setup)
    """
    try:
        # Get plain text input
        input_text = request.get_data(as_text=True)
        
        if not input_text:
            return jsonify({
                'error': 'No input provided'
            }), 400
        
        # Process test cases using Task A logic
        results = process_test_cases(input_text)
        
        # Return with node information
        return jsonify({
            'id': NODE_ID,
            'result': results
        })
        
    except Exception as e:
        return jsonify({
            'error': f'Error processing input: {str(e)}'
        }), 400


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with API information"""
    return jsonify({
        'service': 'Timestamp Difference Calculator',
        'node_id': NODE_ID,
        'endpoints': {
            'POST /calculate': 'Calculate timestamp differences (returns array)',
            'POST /calculate-with-node': 'Calculate with node info (returns object with id and result)',
            'GET /health': 'Health check'
        },
        'input_format': 'Plain text with number of test cases followed by timestamp pairs',
        'example_input': '2\\nSun 10 May 2015 13:54:36 -0700\\nSun 10 May 2015 13:54:36 -0000\\nSat 02 May 2015 19:54:36 +0530\\nFri 01 May 2015 13:54:36 -0000'
    })


if __name__ == '__main__':
    # Get port from environment or default to 5000
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    print(f"Starting API service on {host}:{port}")
    print(f"Node ID: {NODE_ID}")
    
    app.run(host=host, port=port, debug=False)