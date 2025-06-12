#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>

#include <tinyrpc.hpp>

#include "raft.hpp"


ASYNCIO_NS::Task<> async_main(RaftNode::Address addr, const std::string& command) noexcept {
    TINYRPC_NS::Client c;
    if (!co_await c.connect(addr.host.c_str(), addr.port)) {
        SPDLOG_ERROR("connect to {}:{} failed", addr.host, addr.port);
        co_return;
    }
    auto _ = (co_await TINYRPC_NS::call_func<bool>(c, "submit", command))
        .transform([](bool success) {
            if (success) {
                SPDLOG_INFO("submit success");
            } else {
                SPDLOG_INFO("submit failed");
            }
        })
        .or_else([](auto error) -> std::expected<void, TINYRPC_NS::RPCError> {
            switch (error) {
                case TINYRPC_NS::RPCError::ConnectionClosed: {
                    SPDLOG_ERROR("connection closed");
                    break;
                }
                case TINYRPC_NS::RPCError::FunctionNotFound: {
                    SPDLOG_ERROR("function not found");
                    break;
                }
            }
            return {};
        });
}


int main(int argc, char** argv) {
    CLI::App app;
    std::string address { "127.0.0.1:1112" };
    std::string command { "test" };

    argv = app.ensure_utf8(argv);
    app.add_option("--addr,-a", address, "server address");
    app.add_option("--command,-c", command, "command to submit");
    CLI11_PARSE(app, argc, argv);

    ASYNCIO_NS::run(async_main({ address }, command));
}
