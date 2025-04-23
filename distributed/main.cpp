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

class DummyStateMachine : public IStateMachine {
public:
    void apply(const Mutation& mut) override {
        std::cout << "[StateMachine] Applied mutation to file: " << mut.file;
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
        return TableView(); // Dummy impl
    }
};

// Synchronous in-process AppendEntries stub
RaftNode::AppendEntriesReply rpc_append_entries(
    RaftNode* target, const RaftNode::AppendEntriesArgs& args) {
    return target->handle_append_entries(args);
}

int main() {
    std::vector<std::string> ids = {"A", "B", "C"};
    std::vector<std::shared_ptr<RaftNode>> nodes;
    std::vector<std::shared_ptr<DummyStateMachine>> sms;

    // Create nodes
    for (const auto& id : ids) {
        DummyStateMachine* sm = new DummyStateMachine();
        sms.push_back(std::shared_ptr<DummyStateMachine>(sm));
        std::vector<std::string> peers;
        for (const auto& pid : ids) if (pid != id) peers.push_back(pid);
        nodes.push_back(std::make_shared<RaftNode>(id, sms.back(), peers));
    }
    // Start nodes
    for (auto& n : nodes) n->start();

    // Wait for election
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Find the leader
    std::shared_ptr<RaftNode> leader;
    for (auto& n : nodes) {
        if (n->role() == ServerRole::LEADER) {
            leader = n;
            break;
        }
    }
    if (!leader) {
        std::cerr << "No leader elected!" << std::endl;
        for (auto& n : nodes) n->stop();
        return 1;
    }
    std::cout << "Leader is: " << leader->current_leader() << std::endl;

    // Issue a PUT mutation
    Mutation mut;
    mut.file = "foo";
    RowInsert ins;
    ins.values.push_back("bar");
    mut.op = ins;
    leader->submit(mut);

    // Wait for commit/apply
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Stop nodes
    for (auto& n : nodes) n->stop();
    return 0;
}
