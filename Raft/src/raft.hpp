#pragma once
#include <tinyrpc.hpp>

#include "raft_export.hpp"


class RAFT_EXPORT RaftNode {
public:
    using StateMachine = std::function<void(std::string_view)>;
    struct Address {
        std::string host {};
        short port { -1 };

        Address() noexcept = default;
        Address(std::string_view host, short port) noexcept;
        Address(std::string_view addr) noexcept;
        Address(const Address&) noexcept = default;
        Address(Address&&) noexcept = default;
        Address& operator=(const Address&) noexcept = default;
        Address& operator=(Address&&) noexcept = default;

        inline bool operator==(const Address& addr) const noexcept {
            return host == addr.host && port == addr.port;
        }

        inline bool operator<(const Address& addr) const noexcept {
            if (host != addr.host) {
                return host < addr.host;
            }
            return port < addr.port;
        }

        MSGPACK_DEFINE_ARRAY(host, port);
    };

    RaftNode(const std::vector<std::string>& neighbors, StateMachine&& f) noexcept;
    RaftNode(RaftNode&) = delete;
    RaftNode(RaftNode&&) noexcept;
    ~RaftNode() noexcept;
    RaftNode& operator=(RaftNode&) = delete;
    RaftNode& operator=(RaftNode&&) noexcept;
    ASYNCIO_NS::Task<> run(short inner_port, short port, int max_listen_num) noexcept;
private:
    struct impl;
    impl* _pimpl;
};


template<>
struct std::hash<RaftNode::Address> {
    inline size_t operator()(const RaftNode::Address& addr) const noexcept {
        return std::hash<std::string>()(addr.host) ^ std::hash<short>()(addr.port);
    }
};
