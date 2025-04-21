#include "IRaftRPC.hpp"
#include <sstream>

// RequestVoteRequest serialization
std::string RequestVoteRequest::serialize() const {
    std::stringstream ss;
    ss << candidate_id << "|"
       << term << "|"
       << last_log_index << "|"
       << last_log_term;
    return ss.str();
}

// RequestVoteRequest deserialization
RequestVoteRequest RequestVoteRequest::deserialize(const std::string& serialized) {
    std::stringstream ss(serialized);
    RequestVoteRequest request;
    
    std::string term_str, last_log_index_str, last_log_term_str;
    
    std::getline(ss, request.candidate_id, '|');
    std::getline(ss, term_str, '|');
    std::getline(ss, last_log_index_str, '|');
    std::getline(ss, last_log_term_str);
    
    request.term = std::stoull(term_str);
    request.last_log_index = std::stoull(last_log_index_str);
    request.last_log_term = std::stoull(last_log_term_str);
    
    return request;
}

// RequestVoteResponse serialization
std::string RequestVoteResponse::serialize() const {
    std::stringstream ss;
    ss << term << "|"
       << (vote_granted ? "1" : "0");
    return ss.str();
}

// RequestVoteResponse deserialization
RequestVoteResponse RequestVoteResponse::deserialize(const std::string& serialized) {
    std::stringstream ss(serialized);
    RequestVoteResponse response;
    
    std::string term_str, vote_granted_str;
    
    std::getline(ss, term_str, '|');
    std::getline(ss, vote_granted_str);
    
    response.term = std::stoull(term_str);
    response.vote_granted = (vote_granted_str == "1");
    
    return response;
}

// AppendEntriesRequest serialization
std::string AppendEntriesRequest::serialize() const {
    std::stringstream ss;
    ss << leader_id << "|"
       << term << "|"
       << prev_log_index << "|"
       << prev_log_term << "|"
       << leader_commit << "|"
       << entries.size();
    
    // Serialize each log entry
    for (const auto& entry : entries) {
        ss << "|" << entry.serialize();
    }
    
    return ss.str();
}

// AppendEntriesRequest deserialization
AppendEntriesRequest AppendEntriesRequest::deserialize(const std::string& serialized) {
    std::stringstream ss(serialized);
    AppendEntriesRequest request;
    
    std::string term_str, prev_log_index_str, prev_log_term_str, leader_commit_str, entries_count_str;
    
    std::getline(ss, request.leader_id, '|');
    std::getline(ss, term_str, '|');
    std::getline(ss, prev_log_index_str, '|');
    std::getline(ss, prev_log_term_str, '|');
    std::getline(ss, leader_commit_str, '|');
    std::getline(ss, entries_count_str, '|');
    
    request.term = std::stoull(term_str);
    request.prev_log_index = std::stoull(prev_log_index_str);
    request.prev_log_term = std::stoull(prev_log_term_str);
    request.leader_commit = std::stoull(leader_commit_str);
    
    size_t entries_count = std::stoull(entries_count_str);
    request.entries.reserve(entries_count);
    
    // Deserialize each log entry
    for (size_t i = 0; i < entries_count; ++i) {
        std::string entry_str;
        std::getline(ss, entry_str, '|');
        request.entries.push_back(LogEntry::deserialize(entry_str));
    }
    
    return request;
}

// AppendEntriesResponse serialization
std::string AppendEntriesResponse::serialize() const {
    std::stringstream ss;
    ss << term << "|"
       << (success ? "1" : "0") << "|"
       << match_index;
    return ss.str();
}

// AppendEntriesResponse deserialization
AppendEntriesResponse AppendEntriesResponse::deserialize(const std::string& serialized) {
    std::stringstream ss(serialized);
    AppendEntriesResponse response;
    
    std::string term_str, success_str, match_index_str;
    
    std::getline(ss, term_str, '|');
    std::getline(ss, success_str, '|');
    std::getline(ss, match_index_str);
    
    response.term = std::stoull(term_str);
    response.success = (success_str == "1");
    response.match_index = std::stoull(match_index_str);
    
    return response;
}
