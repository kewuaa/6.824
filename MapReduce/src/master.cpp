#include <vector>
#include <ranges>
#include <filesystem>

#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>
#include <tinyrpc.hpp>

#include "types.hpp"
using namespace std::ranges;
namespace fs = std::filesystem;


template<typename... Args>
ASYNCIO_NS::Task<> consume_one(
    std::string func_name,
    ASYNCIO_NS::Condition& cond,
    std::queue<TINYRPC_NS::Client>& workers,
    Args... args
) noexcept {
    if (workers.empty()) {
        co_await cond.wait();
    }
    auto c = std::move(workers.front());
    workers.pop();
    auto task = TINYRPC_NS::call_func<bool>(c, func_name, args...);
    auto timeout = co_await ASYNCIO_NS::wait_for(task, std::chrono::seconds(10));
    if (!timeout && task.result()) {
        workers.push(std::move(c));
        cond.notify_one();
    } else {
        co_await consume_one(func_name, cond, workers, std::move(args)...);
    }
}


class Master {
public:
    inline void register_map_machine(const Address& worker) noexcept {
        if (_map_machines.contains(worker)) {
            SPDLOG_WARN("map machine `{}:{}` already registered", worker.host, worker.port);
            return;
        }
        _map_machines.insert(worker);
        SPDLOG_INFO("register map machine `{}:{}`", worker.host, worker.port);
    }

    inline void register_reduce_machine(const Address& worker) noexcept {
        if (_reduce_machines.contains(worker)) {
            SPDLOG_WARN("reduce machine `{}:{}` already registered", worker.host, worker.port);
            return;
        }
        _reduce_machines.insert(worker);
        SPDLOG_INFO("register reduce machine `{}:{}`", worker.host, worker.port);
    }

    inline void unregister_map_machine(const Address& worker) noexcept {
        if (!_map_machines.contains(worker)) {
            SPDLOG_WARN("map machine `{}:{}` not registered yet", worker.host, worker.port);
            return;
        }
        _map_machines.erase(worker);
        SPDLOG_INFO("unregister map machine `{}:{}`", worker.host, worker.port);
    }

    inline void unregister_reduce_machine(const Address& worker) noexcept {
        if (!_reduce_machines.contains(worker)) {
            SPDLOG_WARN("reduce machine `{}:{}` not registered yet", worker.host, worker.port);
            return;
        }
        _reduce_machines.erase(worker);
        SPDLOG_INFO("unregister reduce machine `{}:{}`", worker.host, worker.port);
    }

    ASYNCIO_NS::Task<void> init_clients(
        const std::unordered_set<Address>& machines,
        std::queue<TINYRPC_NS::Client>& workers
    ) noexcept {
        for (auto& addr : machines) {
            TINYRPC_NS::Client c;
            ASYNCIO_NS::Event<bool> ev;
            auto timeout = co_await ASYNCIO_NS::wait_for(
                c.connect(addr.host.c_str(), addr.port),
                std::chrono::seconds(3)
            );
            if (!timeout) {
                workers.push(std::move(c));
            }
        }
    }

    ASYNCIO_NS::Task<> start_map(
        const std::vector<std::string>& files,
        const std::string& out_dir,
        int reduce_num
    ) noexcept {
        ASYNCIO_NS::Condition cond;
        std::queue<TINYRPC_NS::Client> map_workers;
        co_await init_clients(_map_machines, map_workers);
        auto tasks = files
            | views::transform(
                [&reduce_num, &out_dir, &cond, &map_workers](const std::string& file) {
                    SPDLOG_DEBUG("map {} to {}", file, out_dir);
                    return consume_one(std::string("map"), cond, map_workers, file, out_dir, reduce_num);
                }
            )
            | std::ranges::to<std::vector<ASYNCIO_NS::Task<>>>();
        for (auto& task : tasks) {
            co_await task;
            SPDLOG_INFO("task {} done", task.id());
        }
    }

    ASYNCIO_NS::Task<> start_reduce(int reduce_num, const fs::path& dir) noexcept {
        ASYNCIO_NS::Condition cond;
        std::queue<TINYRPC_NS::Client> reduce_workers;
        co_await init_clients(_reduce_machines, reduce_workers);
        auto tasks = views::iota(0, reduce_num)
            | views::transform([&dir, &cond, &reduce_workers](int idx) {
                std::string in((dir / std::format("reduce-part-{}", idx)).string());
                std::string out((dir / std::format("out-{}", idx)).string());
                SPDLOG_DEBUG("reduce {} to {}", in, out);
                return consume_one(
                    std::string("reduce"),
                    cond,
                    reduce_workers,
                    std::move(in),
                    std::move(out)
                );
            })
            | std::ranges::to<std::vector<ASYNCIO_NS::Task<>>>();
        for (auto& task : tasks) {
            co_await task;
        }
    }

    ASYNCIO_NS::Task<> run(std::vector<std::string> files, std::string out) noexcept {
        int reduce_num = 3;
        fs::path out_dir(out);
        co_await start_map(files, out_dir, reduce_num);
        co_await start_reduce(reduce_num, out_dir);
    }
private:
    std::unordered_set<Address> _map_machines;
    std::unordered_set<Address> _reduce_machines;
};


ASYNCIO_NS::Task<> master_task(const Address& self, int max_listen_num) noexcept {
    Master master;
    auto& rpc = TINYRPC_NS::Server::get();
    TINYRPC_NS::register_func(
        "register_map",
        std::function(
            [&master](const Address& worker) {
                master.register_map_machine(worker);
            }
        )
    );
    TINYRPC_NS::register_func(
        "register_reduce",
        std::function(
            [&master](const Address& worker) {
                master.register_reduce_machine(worker);
            }
        )
    );
    TINYRPC_NS::register_func(
        "unregister_map",
        std::function(
            [&master](const Address& worker) {
                master.unregister_map_machine(worker);
            }
        )
    );
    TINYRPC_NS::register_func(
        "unregister_reduce",
        std::function(
            [&master](const Address& worker) {
                master.unregister_reduce_machine(worker);
            }
        )
    );
    TINYRPC_NS::register_func(
        "run",
        std::function([&master](std::vector<std::string> files, std::string out) {
            master.run(std::move(files), std::move(out));
        })
    );
    rpc.init(self.host.c_str(), self.port, max_listen_num);
    co_await rpc.run();
}


int main(int argc, char** argv) {
    CLI::App app;
    int max_listen_num = 1024;
    Address self("127.0.0.1", 9999);

    argv = app.ensure_utf8(argv);
    app.add_option("-i,--host", self.host, "self host");
    app.add_option("-p,--port", self.port, "self port");
    app.add_option("-l,--listen-number", max_listen_num, "max listen number");
    CLI11_PARSE(app, argc, argv);

    ASYNCIO_NS::run(master_task(self, max_listen_num));
}
