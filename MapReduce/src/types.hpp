#pragma once
#include <string>

#include <msgpack.hpp>


struct Address {
    std::string host;
    short port;
    MSGPACK_DEFINE_ARRAY(host, port)

    inline bool operator==(const Address& addr) const noexcept {
        return host == addr.host && port == addr.port;
    }
};


template<>
struct std::hash<Address> {
    size_t operator()(const Address& addr) const noexcept {
        return hash<std::string>()(addr.host) ^ hash<short>()(addr.port);
    }
};
