#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>

#include "raft.hpp"
#include "raft_config.hpp"


int main(int argc, char** argv) {
    CLI::App app;
    std::vector<std::string> neighbors;
    short port = RAFT_PORT, inner_port = RAFT_INNER_PORT;
    int max_listen_num = RAFT_MAX_LISTEN_NUM;
    std::optional<std::string> log_path = std::nullopt;
    argv = app.ensure_utf8(argv);

    app.add_option("--neighbors,-n", neighbors, "neighbor nodes");
    app.add_option("--port,-p", port, "extern port");
    app.add_option("--inner-port,-i", inner_port, "inner port");
    app.add_option("--log-path,-l", log_path, "log path");
    CLI11_PARSE(app, argc, argv);

    if (log_path) {
        auto logger = spdlog::basic_logger_mt(
            "basic_logger",
            std::format("{}/raft_{}.log", *log_path, inner_port)
        );
        spdlog::set_default_logger(logger);
    }
    spdlog::set_level(spdlog::level::debug);

    RaftNode node(neighbors, [](std::string_view command) {
        fmt::println("===========================================================");
        fmt::println("run command {}", command);
        fmt::println("===========================================================");
    });
    ASYNCIO_NS::run(node.run(inner_port, port, max_listen_num));
}
