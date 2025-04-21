#!/bin/bash

# Script to run a cluster of Raft nodes for the CSV handler system
# This demonstrates two-fault tolerance with persistence

# Create build directory if it doesn't exist
mkdir -p build
cd build

# Compile the code
echo "Compiling the code..."
cmake ..
make

# Check if compilation was successful
if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi

# Define the node addresses (using different ports on localhost for testing)
# In a real deployment, these would be different IP addresses
NODE1_ADDR="127.0.0.1:50051"
NODE2_ADDR="127.0.0.1:50052"
NODE3_ADDR="127.0.0.1:50053"
NODE4_ADDR="127.0.0.1:50054"
NODE5_ADDR="127.0.0.1:50055"

# Create storage directories
mkdir -p node1_data node2_data node3_data node4_data node5_data

# Start the nodes in separate terminals
echo "Starting 5 Raft nodes..."

# Use osascript to open new Terminal windows for each node on macOS
osascript -e "tell application \"Terminal\"
    do script \"cd $(pwd) && ./test_fault_tolerance node1 ./node1_data $NODE1_ADDR $NODE2_ADDR $NODE3_ADDR $NODE4_ADDR $NODE5_ADDR\"
end tell"

sleep 1

osascript -e "tell application \"Terminal\"
    do script \"cd $(pwd) && ./test_fault_tolerance node2 ./node2_data $NODE2_ADDR $NODE1_ADDR $NODE3_ADDR $NODE4_ADDR $NODE5_ADDR\"
end tell"

sleep 1

osascript -e "tell application \"Terminal\"
    do script \"cd $(pwd) && ./test_fault_tolerance node3 ./node3_data $NODE3_ADDR $NODE1_ADDR $NODE2_ADDR $NODE4_ADDR $NODE5_ADDR\"
end tell"

sleep 1

osascript -e "tell application \"Terminal\"
    do script \"cd $(pwd) && ./test_fault_tolerance node4 ./node4_data $NODE4_ADDR $NODE1_ADDR $NODE2_ADDR $NODE3_ADDR $NODE5_ADDR\"
end tell"

sleep 1

osascript -e "tell application \"Terminal\"
    do script \"cd $(pwd) && ./test_fault_tolerance node5 ./node5_data $NODE5_ADDR $NODE1_ADDR $NODE2_ADDR $NODE3_ADDR $NODE4_ADDR\"
end tell"

echo "All nodes started. To test fault tolerance:"
echo "1. Wait for leader election to complete (check the terminal outputs)"
echo "2. Kill 1-2 nodes (including the leader if desired) using Ctrl+C"
echo "3. Observe how the system recovers and elects a new leader"
echo "4. Restart the killed nodes to see how they catch up"
echo ""
echo "The system can tolerate up to 2 node failures while maintaining consensus."
