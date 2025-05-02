#include "raft_node.hpp"
#include "core/IStateMachine.hpp"
#include "core/Mutation.hpp"
#include "core/TableView.hpp"
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <random>
#include <algorithm>
#include <signal.h>
#include <mutex>

// Simple in-memory state machine implementation for testing
class TestStateMachine : public IStateMachine {
public:
    TestStateMachine(const std::string& id) : id_(id) {}
    
    void apply(const Mutation& mut) override {
        std::lock_guard<std::mutex> lock(mutex_);
        mutations_.push_back(mut);
        std::cout << "[Node " << id_ << " StateMachine] Applied mutation to file: " << mut.file;
        if (mut.has_insert()) {
            const auto& ins = mut.insert();
            std::cout << " [insert: ";
            for (const auto& val : ins.values) std::cout << val << ",";
            std::cout << "]";
        } else if (mut.has_delete()) {
            const auto& del = mut.del();
            std::cout << " [delete row: " << del.row_index << "]";
        }
        std::cout << std::endl;
    }
    
    TableView view(const std::string& file) const override {
        // Simple implementation - just return a dummy TableView
        return TableView();
    }
    
    size_t mutation_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return mutations_.size();
    }
    
private:
    std::string id_;
    std::vector<Mutation> mutations_;
    mutable std::mutex mutex_;
};

// Synchronous in-process RPC stubs for direct method calls
RaftNode::AppendEntriesReply append_entries_rpc(RaftNode* target, const RaftNode::AppendEntriesArgs& args) {
    return target->handle_append_entries(args);
}

RaftNode::RequestVoteReply request_vote_rpc(RaftNode* target, const RaftNode::RequestVoteArgs& args) {
    return target->handle_request_vote(args);
}

// Mock RaftServer that uses direct method calls instead of gRPC
class InProcessRaftServer {
public:
    InProcessRaftServer(std::shared_ptr<RaftNode> node, const std::string& address)
        : node_(node), server_address_(address) {}
    
    void register_peer(const std::string& address, RaftNode* peer) {
        peer_map_[address] = peer;
    }
    
    std::shared_ptr<RaftNode> node() const { return node_; }
    std::string address() const { return server_address_; }
    
    // Called by other InProcessRaftServers to send AppendEntries
    RaftNode::AppendEntriesReply send_append_entries(const RaftNode::AppendEntriesArgs& args) {
        return node_->handle_append_entries(args);
    }
    
    // Called by other InProcessRaftServers to send RequestVote
    RaftNode::RequestVoteReply send_request_vote(const RaftNode::RequestVoteArgs& args) {
        return node_->handle_request_vote(args);
    }
    
    // Methods for node to send RPCs to other servers
    RaftNode::AppendEntriesReply send_append_entries_to(
        const std::string& target_address, const RaftNode::AppendEntriesArgs& args) {
        auto it = peer_map_.find(target_address);
        if (it != peer_map_.end()) {
            return append_entries_rpc(it->second, args);
        }
        
        // Simulate network failure
        RaftNode::AppendEntriesReply failure;
        failure.success = false;
        return failure;
    }
    
    RaftNode::RequestVoteReply send_request_vote_to(
        const std::string& target_address, const RaftNode::RequestVoteArgs& args) {
        auto it = peer_map_.find(target_address);
        if (it != peer_map_.end()) {
            return request_vote_rpc(it->second, args);
        }
        
        // Simulate network failure
        RaftNode::RequestVoteReply failure;
        failure.vote_granted = false;
        return failure;
    }
    
private:
    std::shared_ptr<RaftNode> node_;
    std::string server_address_;
    std::unordered_map<std::string, RaftNode*> peer_map_;
};

// Flag to indicate if we should exit
std::atomic<bool> should_exit(false);

// Signal handler for graceful shutdown
void signal_handler(int signal) {
    std::cout << "Received signal " << signal << ", initiating shutdown..." << std::endl;
    should_exit = true;
}

int main(int argc, char* argv[]) {
    // Register signal handler
    signal(SIGINT, signal_handler);
    
    // Number of nodes in the cluster
    const int NODE_COUNT = 3;
    
    // Base port for servers (just for addressing, not actual networking)
    const int BASE_PORT = 50051;
    
    // Create node IDs and addresses
    std::vector<std::string> node_ids;
    std::vector<std::string> addresses;
    
    for (int i = 0; i < NODE_COUNT; ++i) {
        node_ids.push_back("node" + std::to_string(i));
        addresses.push_back("localhost:" + std::to_string(BASE_PORT + i));
    }
    
    // Create state machines, nodes, and servers
    std::vector<std::shared_ptr<TestStateMachine>> state_machines;
    std::vector<std::shared_ptr<RaftNode>> nodes;
    std::vector<std::shared_ptr<InProcessRaftServer>> servers;
    
    std::cout << "=== Starting In-Process Raft Cluster Demo ===" << std::endl;
    std::cout << "Press Ctrl+C to exit." << std::endl << std::endl;
    
    // Initialize nodes and servers
    for (int i = 0; i < NODE_COUNT; ++i) {
        // Create state machine
        auto sm = std::make_shared<TestStateMachine>(node_ids[i]);
        state_machines.push_back(sm);
        
        // Create peer list (all other nodes)
        std::vector<std::string> peers;
        for (int j = 0; j < NODE_COUNT; ++j) {
            if (j != i) {
                peers.push_back(addresses[j]);
            }
        }
        
        // Create node
        auto node = std::make_shared<RaftNode>(node_ids[i], sm, peers);
        nodes.push_back(node);
        
        // Create server
        auto server = std::make_shared<InProcessRaftServer>(node, addresses[i]);
        servers.push_back(server);
    }
    
    // Register peers in each server
    for (int i = 0; i < NODE_COUNT; ++i) {
        for (int j = 0; j < NODE_COUNT; ++j) {
            if (i != j) {
                servers[i]->register_peer(addresses[j], nodes[j].get());
            }
        }
    }
    
    // Start all nodes
    for (auto& node : nodes) {
        node->start();
    }
    
    std::cout << "All nodes started. Waiting for leader election..." << std::endl;
    
    // Wait for leader election (3 seconds)
    std::this_thread::sleep_for(std::chrono::seconds(3));
    
    // Find the leader
    std::shared_ptr<RaftNode> leader;
    int leader_index = -1;
    
    for (int i = 0; i < NODE_COUNT; ++i) {
        if (nodes[i]->role() == ServerRole::LEADER) {
            leader = nodes[i];
            leader_index = i;
            break;
        }
    }
    
    if (!leader) {
        std::cerr << "No leader elected! Exiting..." << std::endl;
        
        // Stop all nodes
        for (auto& node : nodes) {
            node->stop();
        }
        
        return 1;
    }
    
    std::cout << std::endl;
    std::cout << "Leader elected: " << leader->current_leader() << std::endl;
    std::cout << "Submitting test mutations..." << std::endl;
    
    // Submit test mutations to the leader
    Mutation insert1;
    insert1.file = "test.csv";
    RowInsert row1;
    row1.values = {"Alice", "30", "Engineer"};
    insert1.op = row1;
    
    Mutation insert2;
    insert2.file = "test.csv";
    RowInsert row2;
    row2.values = {"Bob", "25", "Designer"};
    insert2.op = row2;
    
    Mutation del;
    del.file = "test.csv";
    RowDelete row_del;
    row_del.row_index = 0;
    del.op = row_del;
    
    // Submit and wait
    leader->submit(insert1);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    leader->submit(insert2);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    leader->submit(del);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Wait for some time to let the changes propagate
    std::cout << std::endl;
    std::cout << "Waiting for mutations to be replicated and applied..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Check state machine state on all nodes
    std::cout << std::endl;
    std::cout << "=== Final State Machine Status ===" << std::endl;
    for (int i = 0; i < NODE_COUNT; ++i) {
        std::cout << "Node " << node_ids[i] << " (";
        switch (nodes[i]->role()) {
            case ServerRole::LEADER: std::cout << "LEADER"; break;
            case ServerRole::FOLLOWER: std::cout << "FOLLOWER"; break;
            case ServerRole::CANDIDATE: std::cout << "CANDIDATE"; break;
            case ServerRole::STANDALONE: std::cout << "STANDALONE"; break;
        }
        std::cout << ") has applied " << state_machines[i]->mutation_count() 
                  << " mutations." << std::endl;
    }
    
    // Enter main loop waiting for user to terminate
    std::cout << std::endl;
    std::cout << "Cluster is running. Press Ctrl+C to exit." << std::endl;
    
    // Wait until should_exit is set by signal handler
    while (!should_exit) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        // Check for leader changes every 5 seconds
        static int counter = 0;
        if (++counter >= 5) {
            counter = 0;
            
            std::string current_leader;
            for (int i = 0; i < NODE_COUNT; ++i) {
                if (nodes[i]->role() == ServerRole::LEADER) {
                    current_leader = node_ids[i];
                    break;
                }
            }
            
            if (!current_leader.empty()) {
                std::cout << "Current leader: " << current_leader << std::endl;
            } else {
                std::cout << "No leader currently!" << std::endl;
            }
        }
    }
    
    std::cout << "Shutting down cluster..." << std::endl;
    
    // Stop all nodes
    for (auto& node : nodes) {
        node->stop();
    }
    
    std::cout << "Shutdown complete." << std::endl;
    
    return 0;
}
