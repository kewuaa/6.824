#include <random>
#include <ranges>
#include <utility>
#include <functional>

#include <spdlog/spdlog.h>

#include "utils.hpp"
#include "raft.hpp"
using namespace std::ranges;


RaftNode::Address::Address(std::string_view host, short port) noexcept:
    host(host), port(port) {}

RaftNode::Address::Address(std::string_view addr) noexcept {
    auto pos = addr.find(':');
    if (pos == std::string_view::npos) {
        SPDLOG_ERROR("failed to parse address {}", addr);
        exit(EXIT_FAILURE);
    }
    host = addr.substr(0, pos);
    auto [ptr, ec] = std::from_chars(
        addr.data()+pos+1,
        addr.data()+addr.size(),
        port
    );
    if (ec != std::errc()) {
        SPDLOG_ERROR("failed to parse port: {}", std::make_error_code(ec).message());
        exit(EXIT_FAILURE);
    }
}


struct RaftNode::impl {
    using TermID = size_t;
    enum class Role {
        Follower,
        Candidator,
        Leader
    };
    struct NodeInfo {
        std::optional<TINYRPC_NS::Client> client { std::nullopt };
        bool is_syncing { false };
        size_t next_idx { 0 };
    };
    struct Entry {
        TermID term;
        std::string command;
        MSGPACK_DEFINE_ARRAY(term, command);

        ASYNCIO_NS::Event<std::string>* ev { nullptr };
    };
    struct VoteRequest {
        Address addr;
        TermID term;
        size_t commit_idx;
        MSGPACK_DEFINE_ARRAY(addr, term, commit_idx);
    };
    struct VoteResponse {
        TermID term;
        bool granted;
        MSGPACK_DEFINE_ARRAY(granted, term);
    };
    struct AppendEntryRequest {
        TermID term;
        Address addr;
        size_t prev_entry_idx;
        TermID prev_entry_term;
        size_t leader_commit_idx;
        std::vector<Entry> entries;
        MSGPACK_DEFINE_ARRAY(
            term,
            addr,
            prev_entry_idx,
            prev_entry_term,
            leader_commit_idx,
            entries
        );
    };
    struct AppendEntryResponse {
        TermID term;
        bool success;
        MSGPACK_DEFINE_ARRAY(term, success);
    };

    struct NodeCompare {
        bool operator()(const NodeInfo* node1, const NodeInfo* node2) const noexcept {
            return node1->next_idx > node2->next_idx;
        }
    };

    std::mt19937 random_engine { std::random_device()() };

    TermID term { 0 };
    Address addr { "127.0.0.1", -1 };
    // std::string host {};
    // short port { -1 }, inner_port { -1 };
    Role role { Role::Follower };
    std::vector<Entry> logs { 1, { 0, "", nullptr } };
    size_t entry_idx { 1 };
    size_t commit_idx { 1 };
    ASYNCIO_NS::Event<bool> commit_event {};
    std::optional<ASYNCIO_NS::Task<>> commit_task { std::nullopt };
    std::optional<Address> voted_node { std::nullopt };
    std::optional<Address> leader { std::nullopt };
    std::map<Address, NodeInfo> neighbors {};
    std::shared_ptr<ASYNCIO_NS::Timer> election_timer { nullptr };
    std::shared_ptr<ASYNCIO_NS::Timer> heart_beat_timer { nullptr };

    StateMachine state_machine { nullptr };

    impl(const std::vector<std::string>& neighbors, StateMachine&& f) noexcept: state_machine(std::move(f)) {
        for (auto& addr_str : neighbors) {
            this->neighbors.insert({ Address(addr_str), {} });
            SPDLOG_INFO("add node: {}", addr_str);
        }
        become_follower();
        commit_task = commit_log();
    }

    impl(impl&) = delete;
    impl(impl&&) = default;
    impl& operator=(impl&) = delete;
    impl& operator=(impl&&) = default;

    ~impl() noexcept {
        if (commit_task) {
            commit_task->cancel();
            commit_task.reset();
        }
    }

    inline void update_term(TermID term) {
        if (this->term != term) {
            SPDLOG_INFO("update term from {} to {}", this->term, term);
            this->term = term;
            voted_node.reset();
            leader.reset();
        }
    }

    void reset_election_timer() noexcept {
        static std::uniform_int_distribution<> dist(1500, 2000);
        SPDLOG_INFO("reset election timer");
        if (election_timer != nullptr) {
            election_timer->cancel();
            election_timer.reset();
        }
        auto timeout = std::chrono::milliseconds(dist(random_engine));
        SPDLOG_DEBUG("election timer timeout: {} ms", timeout.count());
        election_timer = ASYNCIO_NS::EventLoop::get().call_later(
            timeout,
            [this] {
                election_timer.reset();
                become_candidator();
            }
        );
    }

    void reset_heart_beat_timer() noexcept {
        SPDLOG_INFO("reset heart beat timer");
        if (heart_beat_timer != nullptr) {
            heart_beat_timer->cancel();
            heart_beat_timer.reset();
        }
        heart_beat_timer = ASYNCIO_NS::EventLoop::get().call_later(
            std::chrono::milliseconds(500),
            [this] {
                heart_beat_timer.reset();
                heart_beat<false>();
            }
        );
    }

    ASYNCIO_NS::Task<> commit_log() noexcept {
        while (true) {
            auto end_idx = std::min(commit_idx, logs.size());
            for (; entry_idx < end_idx; ++entry_idx) {
                // commit one log entry
                auto& entry = logs[entry_idx];

                auto res = state_machine(entry.command);
                SPDLOG_INFO("commit entry ({}, {}) successfully", entry_idx, entry.command);

                // try to wake up submit task
                if (auto ev = std::exchange(entry.ev, nullptr); ev) {
                    ev->set(std::move(res));;
                }
            }
            auto stop = co_await commit_event.wait();
            if (stop && *stop) {
                break;
            }
        }
    }

    void become_follower() noexcept {
        SPDLOG_INFO("become follower");
        role = Role::Follower;
        // try to cancel heart beat timer
        if (heart_beat_timer) {
            SPDLOG_INFO("cancel heart beat timer");
            heart_beat_timer->cancel();
            heart_beat_timer.reset();
        }
        reset_election_timer();
    }

    void become_leader() noexcept {
        SPDLOG_INFO("become leader");
        role = Role::Leader;
        for (auto& [_, node] : neighbors) {
            node.is_syncing = false;
            node.next_idx = logs.size();
        }
        // heart beat right now
        heart_beat<true>();
    }

    ASYNCIO_NS::Task<> become_candidator() noexcept {
        ++term;
        SPDLOG_INFO("become candidator, term: {}", term);
        role = Role::Candidator;
        size_t vote_count = 1;
        size_t thr = (neighbors.size() + 1) / 2;
        // save all request
        auto req = VoteRequest(addr, term, logs.size());
        std::vector<ASYNCIO_NS::Task<VoteResponse, TINYRPC_NS::RPCError>> tasks;
        const std::string func_name  { "ask_for_vote" };
        // ask each node for vote request
        for (auto& [addr, node] : neighbors) {
            if (!node.client) {
                SPDLOG_INFO("try connect to {}:{}", addr.host, addr.port);
                TINYRPC_NS::Client c;
                if (co_await c.connect(addr.host.c_str(), addr.port)) {
                    node.client = std::move(c);
                } else {
                    SPDLOG_INFO("node {}:{} no response, skip", addr.host, addr.port);
                    continue;
                }
            }
            auto task = TINYRPC_NS::call_func<VoteResponse>(*node.client, func_name, req);
            task.add_done_callback(
                [this, thr, &vote_count, &addr, &node](const decltype(task)::result_type& res) {
                    // maybe connection closed
                    // reset client
                    if (!res) {
                        node.client.reset();
                        return;
                    }

                    // if already become follower or leader, directly return
                    if (role != Role::Candidator) return;

                    const auto& resp = *res;

                    // vote not be granted
                    if (!resp.granted) {
                        SPDLOG_INFO("vote refused by {}:{}", addr.host.c_str(), addr.port);
                        // term smaller than other, upadte term and become follower
                        if (resp.term > term) {
                            term = resp.term;
                            become_follower();
                        }
                        return;
                    }

                    // vote count plus 1
                    ++vote_count;

                    // vote enough, become leader
                    if (vote_count > thr) {
                        become_leader();
                    };
                }
            );
            tasks.emplace_back(std::move(task));
        }
        // wait for all request done
        for (auto& task : tasks) {
            // already become follower or leader, no need to wait
            if (role != Role::Candidator) {
                break;
            }
            if constexpr (RAFT_WAIT_TIMEOUT > 0) {
                auto timeout = co_await ASYNCIO_NS::wait_for(task, std::chrono::milliseconds(RAFT_WAIT_TIMEOUT));
                if (timeout) {
                    SPDLOG_INFO("wait for vote task timeout");
                }
            } else {
                co_await task;
            }
        }
        // election failed, become follower
        if (role == Role::Candidator) {
            SPDLOG_INFO("election failed");
            become_follower();
        }
    }

    ASYNCIO_NS::Task<> sync(const Address& addr, NodeInfo& node, bool no_entry) noexcept {
        SPDLOG_DEBUG("================sync to {}:{}====================", addr.host, addr.port);
        SPDLOG_DEBUG("next index  : {}", node.next_idx);
        SPDLOG_DEBUG("commit index: {}", commit_idx);
        SPDLOG_DEBUG("=================================================");
        if (!node.client) {
            TINYRPC_NS::Client c;
            if (co_await c.connect(addr.host.c_str(), addr.port)) {
                node.client = std::move(c);
            } else {
                SPDLOG_INFO("node {}:{} no response, skip", addr.host, addr.port);
                co_return;
            }
        }

        AppendEntryRequest req(
            term,
            this->addr,
            node.next_idx-1,
            logs[node.next_idx-1].term,
            commit_idx,
            no_entry || node.is_syncing // if during syncing, not send entries
                ? std::vector<Entry>()
                : std::vector<Entry>(
                    logs.begin()+node.next_idx,
                    logs.end()
                )
        );
        bool own_node = false;
        // entries not empty, own node and set syncing to avoid owned by others
        if (!req.entries.empty()) {
            own_node = true;
            node.is_syncing = true;
        }
        while (role == Role::Leader) {
            SPDLOG_INFO("try to append entry to {}:{}", addr.host, addr.port);
            auto res = co_await TINYRPC_NS::call_func<AppendEntryResponse>(
                *node.client,
                "ask_for_append_entry",
                req
            );

            // maybe connection closed
            // reset client
            if (!res) {
                SPDLOG_INFO("connection to {}:{} closed", addr.host, addr.port);
                node.client.reset();
                break;
            }

            // maybe become follower, no need to continue
            if (role != Role::Leader) break;

            auto& resp = *res;

            // logs incomplete or inconsistent
            if (!resp.success) {
                SPDLOG_INFO("append entry to {}:{} failed", addr.host, addr.port);
                // old term, update term and become follower and break
                if (resp.term > term) {
                    term = resp.term;
                    become_follower();
                    break;
                }
                // not own node, try own node or else no need to retry
                if (!own_node) {
                    if (node.is_syncing) {
                        break;
                    }
                    own_node = true;
                    node.is_syncing = true;
                }
                assert(node.next_idx > 0);
                // log back
                // adjust previous entry index and retry
                // no need to transport entries
                --node.next_idx;
                req.prev_entry_idx = node.next_idx - 1;
                req.prev_entry_term = logs[node.next_idx-1].term;
                if (!req.entries.empty()) {
                    req.entries.resize(0);
                }
                continue;
            }

            if (!req.entries.empty()) {
                // update next index
                node.next_idx += req.entries.size();
            }
            SPDLOG_INFO("sync to {}:{} successfully", addr.host, addr.port);
            break;
        }
        // if own node, unset syncing
        if (own_node) {
            node.is_syncing = false;
        }
    }

    void update_commit_idx() noexcept {
        // find new commit index
        auto thr = (neighbors.size() + 1) / 2;
        auto size = thr + 1;
        std::priority_queue<NodeInfo*, std::vector<NodeInfo*>, NodeCompare> cache;
        for (auto& [_, node] : neighbors) {
            if (cache.size() < thr) {
                cache.push(&node);
            } else if (node.next_idx > cache.top()->next_idx) {
                cache.pop();
                cache.push(&node);
            }
        }
        if (cache.top()->next_idx != commit_idx) {
            // update commit index
            SPDLOG_INFO("update commit index from {} to {}", commit_idx, cache.top()->next_idx);
            commit_idx = cache.top()->next_idx;
            // wake up to commit
            if (!commit_event.is_set()) {
                commit_event.set();
            }
        }
    }

    template<bool FirstTime>
    std::conditional_t<FirstTime, void, ASYNCIO_NS::Task<>> heart_beat() noexcept {
        SPDLOG_INFO("heart beat");
        // ask each node to append entry
        std::vector<ASYNCIO_NS::Task<>> tasks;
        tasks.reserve(neighbors.size());
        for (auto& [addr, node] : neighbors) {
            tasks.emplace_back(sync(addr, node, FirstTime));
        }

        // reset heart beat timer if still leader
        if (role == Role::Leader) {
            reset_heart_beat_timer();
        }

        if constexpr (!FirstTime) {
            // wait for log sync done
            for (auto& task : tasks) {
                if constexpr (RAFT_WAIT_TIMEOUT > 0) {
                    auto timeout = co_await ASYNCIO_NS::wait_for(task, std::chrono::milliseconds(RAFT_WAIT_TIMEOUT));
                    if (timeout) {
                        SPDLOG_INFO("wait for sync task timeout");
                    }
                } else {
                    co_await task;
                }
            }

            // update commit index
            update_commit_idx();
        }
    }

    VoteResponse ask_for_vote(const VoteRequest& req) noexcept {
        SPDLOG_DEBUG("=========================receive vote request===========================");
        SPDLOG_DEBUG("candidator term        : {}", req.term);
        SPDLOG_DEBUG("candidator address     : {}:{}", req.addr.host, req.addr.port);
        SPDLOG_DEBUG("candidator commit index: {}", req.commit_idx);
        SPDLOG_DEBUG("========================================================================");
        VoteResponse resp { req.term, false };

        // old term vote, refuse it
        if (req.term < term) {
            resp.term = term;
            return resp;
        }

        // update current term, if different voted_node will be reset
        update_term(req.term);

        // candidator or have already voted
        if (role == Role::Candidator || voted_node) return resp;

        // old commit log
        if (req.commit_idx < logs.size()) return resp;

        // vote and become follower
        resp.granted = true;
        voted_node = req.addr;
        become_follower();
        return resp;
    }

    AppendEntryResponse ask_for_append_entry(AppendEntryRequest&& req) noexcept {
        SPDLOG_DEBUG("================receive append entry request============================");
        SPDLOG_DEBUG("leader term          : {}", req.term);
        SPDLOG_DEBUG("leader address       : {}:{}", req.addr.host, req.addr.port);
        SPDLOG_DEBUG("leader previous entry: term {}, index {}", req.prev_entry_term, req.prev_entry_idx);
        SPDLOG_DEBUG("leader commit index  : {}", req.leader_commit_idx);
        for (auto& entry : req.entries) {
            SPDLOG_DEBUG("entry                : ({}, {})", entry.term, entry.command);
        }
        SPDLOG_DEBUG("========================================================================");
        AppendEntryResponse resp { req.term, false };

        // old term, not accept
        if (req.term < term) {
            resp.term = term;
            return resp;
        }

        // update current term, if different voted_node will be reset
        update_term(req.term);

        // receive heart beat, become follower and reset election timer
        become_follower();

        // set leader
        if (!leader) {
            leader = req.addr;
        }

        if (
            // logs are incomplete
            req.prev_entry_idx >= logs.size()
            // logs inconsistent
            || logs[req.prev_entry_idx].term != req.prev_entry_term
        ) return resp;

        // erase inconsistent log
        logs.erase(logs.begin()+req.prev_entry_idx+1, logs.end());

        // sync logs
        if (!req.entries.empty()) {
            for (auto& entry : req.entries) {
                logs.push_back(std::move(entry));
            }
        }

        // update commit index with leader
        if (req.leader_commit_idx > commit_idx) {
            commit_idx = req.leader_commit_idx;
        }

        // wake up to commit
        if (entry_idx < std::min(commit_idx, logs.size()) && !commit_event.is_set()) {
            commit_event.set();
        }

        // accept request
        resp.success = true;
        return resp;
    }

    ASYNCIO_NS::Task<raft::proto::SubmitResult> submit(std::string command) noexcept {
        raft::proto::SubmitResult res;
        res.set_err(raft::proto::SubmitResult_Error_LeaderNoResponse);
        if (role == Role::Leader) {
            SPDLOG_INFO("submit command: {}", command);
            ASYNCIO_NS::Event<std::string> ev;
            logs.emplace_back(term, std::move(command), &ev);
            res.clear_err();
            res.set_data(*(co_await ev.wait()));
            co_return std::move(res);
        }
        if (!leader || !neighbors.contains(*leader)) {
            SPDLOG_WARN("no leader");
            res.set_err(raft::proto::SubmitResult_Error_NoLeader);
            co_return std::move(res);
        }
        auto& node = neighbors[*leader];
        if (!node.client) {
            auto& addr = *leader;
            SPDLOG_INFO("try connect to leader {}:{}", addr.host, addr.port);
            TINYRPC_NS::Client c;
            if (co_await c.connect(addr.host.c_str(), addr.port)) {
                node.client = std::move(c);
            } else {
                SPDLOG_INFO("leader {}:{} no response, skip", addr.host, addr.port);
                co_return std::move(res);
            }
        }
        SPDLOG_INFO("redirect command `{}` to leader {}:{}", command, leader->host, leader->port);
        co_return (co_await TINYRPC_NS::call_func<raft::proto::SubmitResult>(
            *node.client,
            "inner_submit",
            std::move(command)
        )).value_or(std::move(res));
    }

    ASYNCIO_NS::Task<> run_inner_rpc(const char* host, short port, int max_listen_num) noexcept {
        addr = { host, port };
        TINYRPC_NS::Server rpc;
        rpc.init(host, port, RAFT_MAX_LISTEN_NUM);
        TINYRPC_NS::register_func(
            rpc,
            "ask_for_vote",
            std::function(
                [this](const impl::VoteRequest& req) {
                    return ask_for_vote(req);
                }
            )
        );
        TINYRPC_NS::register_func(
            rpc,
            "ask_for_append_entry",
            std::function(
                [this](impl::AppendEntryRequest&& req) {
                    return ask_for_append_entry(std::move(req));
                }
            )
        );
        TINYRPC_NS::register_func(
            rpc,
            "inner_submit",
            std::function(
                [this](std::string&& command) {
                    return submit(std::move(command));
                }
            )
        );
        co_await rpc.run();
    }

    ASYNCIO_NS::Task<> run_extern_rpc(const char* host, short port, int max_listen_num) noexcept {
        TINYRPC_NS::Server rpc;
        rpc.init(host, port, RAFT_MAX_LISTEN_NUM);
        TINYRPC_NS::register_func(
            rpc,
            "submit",
            std::function(
                [this](std::string&& command) {
                    return submit(std::move(command));
                }
            )
        );
        co_await rpc.run();
    }
};


RaftNode::RaftNode(const std::vector<std::string>& neighbors, StateMachine&& f) noexcept:
    _pimpl(new impl(neighbors, std::move(f))) {}

RaftNode::RaftNode(RaftNode&& node) noexcept:
    _pimpl(std::exchange(node._pimpl, nullptr)) {}

RaftNode& RaftNode::operator=(RaftNode&& node) noexcept {
    free_and_null(_pimpl);
    _pimpl = std::exchange(node._pimpl, nullptr);
    return *this;
}

RaftNode::~RaftNode() noexcept {
    free_and_null(_pimpl);
}

ASYNCIO_NS::Task<> RaftNode::run(const char* host, short inner_port, short port, int max_listen_num) noexcept {
    for (auto& task : {
        _pimpl->run_inner_rpc(host, inner_port, max_listen_num),
        _pimpl->run_extern_rpc(host, port, max_listen_num),
    }) {
        co_await task;
    }
}
