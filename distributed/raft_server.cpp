#include "raft_server.hpp"
#include "proto/raft_service.grpc.pb.h"
#include <iostream>

// Inner class implementation of the gRPC service
class RaftServer::RaftServiceImpl final : public raft::RaftService::Service {
public:
    RaftServiceImpl(std::shared_ptr<RaftNode> node) : node_(node) {}

    // AppendEntries RPC implementation
    grpc::Status AppendEntries(grpc::ServerContext* context,
                             const raft::AppendEntriesRequest* request,
                             raft::AppendEntriesResponse* response) override {
        // Convert gRPC request to internal AppendEntriesArgs
        RaftNode::AppendEntriesArgs args;
        args.term = request->term();
        args.leader_id = request->leader_id();
        args.prev_log_index = request->prev_log_index();
        args.prev_log_term = request->prev_log_term();
        args.leader_commit = request->leader_commit();
        
        // Convert log entries
        for (const auto& entry : request->entries()) {
            RaftNode::LogEntry log_entry;
            log_entry.term = entry.term();
            
            // Convert mutation
            mutation::Mutation proto_mut = entry.cmd();
            Mutation mut;
            mut.file = proto_mut.file();
            
            // Handle insert or delete operation
            if (proto_mut.has_insert()) {
                RowInsert insert;
                for (const auto& value : proto_mut.insert().values()) {
                    insert.values.push_back(value);
                }
                mut.op = insert;
            } else if (proto_mut.has_delete()) {
                RowDelete del;
                del.row_index = proto_mut.delete_().row_index();
                mut.op = del;
            }
            
            log_entry.cmd = mut;
            args.entries.push_back(log_entry);
        }
        
        // Call node's AppendEntries handler
        RaftNode::AppendEntriesReply reply = node_->handle_append_entries(args);
        
        // Convert reply to gRPC response
        response->set_term(reply.term);
        response->set_success(reply.success);
        response->set_conflict_index(reply.conflict_index);
        response->set_conflict_term(reply.conflict_term);
        
        return grpc::Status::OK;
    }
    
    // RequestVote RPC implementation
    grpc::Status RequestVote(grpc::ServerContext* context,
                           const raft::RequestVoteRequest* request,
                           raft::RequestVoteResponse* response) override {
        // Convert gRPC request to internal RequestVoteArgs
        RaftNode::RequestVoteArgs args;
        args.term = request->term();
        args.candidate_id = request->candidate_id();
        args.last_log_index = request->last_log_index();
        args.last_log_term = request->last_log_term();
        
        // Call node's RequestVote handler
        RaftNode::RequestVoteReply reply = node_->handle_request_vote(args);
        
        // Convert reply to gRPC response
        response->set_term(reply.term);
        response->set_vote_granted(reply.vote_granted);
        
        return grpc::Status::OK;
    }

private:
    std::shared_ptr<RaftNode> node_;
};

// RaftServer implementation
RaftServer::RaftServer(std::shared_ptr<RaftNode> node, const std::string& address)
    : node_(node), server_address_(address), service_(new RaftServiceImpl(node)) {
}

void RaftServer::start() {
    grpc::ServerBuilder builder;
    
    // Listen on the given address without any authentication
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    
    // Register the service
    builder.RegisterService(service_.get());
    
    // Finally assemble the server
    server_ = builder.BuildAndStart();
    std::cout << "RaftServer listening on " << server_address_ << std::endl;
}

void RaftServer::stop() {
    if (server_) {
        std::cout << "Stopping RaftServer at " << server_address_ << std::endl;
        server_->Shutdown();
    }
}

std::string RaftServer::address() const {
    return server_address_;
}
