#!/bin/bash

# Kill any existing server processes
pkill -f "./server"

# Start the leader node
echo "Starting leader node on port 50051..."
./build/server --port 50051 --self_address localhost:50051 &
sleep 2  # Wait for the leader to start

# Start the first follower
echo "Starting follower 1 on port 50052..."
./build/server --port 50052 --self_address localhost:50052 --leader_address localhost:50051 &
sleep 1  # Wait for the follower to start

# Start the second follower
echo "Starting follower 2 on port 50053..."
./build/server --port 50053 --self_address localhost:50053 --leader_address localhost:50051 &

echo "Cluster started. Use the following commands to connect to the nodes:"
echo "  ./build/client --server_address localhost:50051  # Connect to leader"
echo "  ./build/client --server_address localhost:50052  # Connect to follower 1"
echo "  ./build/client --server_address localhost:50053  # Connect to follower 2"
