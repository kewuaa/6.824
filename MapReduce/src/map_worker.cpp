#include <cstdio>
#include <fstream>
#include <filesystem>

#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>
#include <tinyrpc.hpp>

#include "types.hpp"
using namespace std::ranges;
namespace fs = std::filesystem;


bool map_func(std::string in, std::string out, int reduce_count) noexcept {
    if (!fs::exists(in)) {
        return false;
    }
    std::ifstream fin(in);
    fs::path out_dir(out);
    char c;
    std::string word;
    std::hash<std::string> hash_fn;
    std::vector<std::vector<std::string>> outs;
    outs.resize(reduce_count);
    while ((c = fin.get()) != EOF) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            word.push_back(c);
        } else {
            if (!word.empty()) {
                auto idx = hash_fn(word) % reduce_count;
                outs[idx].push_back(std::exchange(word, {}));
            }
        }
    }
    for (int i = 0; i < reduce_count; ++i) {
        if (outs[i].empty()) {
            continue;
        }
        std::ofstream f(
            out_dir / std::format("reduce-part-{}", i),
            std::ios::app
        );
        for (auto& word : outs[i]) {
            f << word << std::endl;
        }
    }
    return true;
}


ASYNCIO_NS::Task<> map_task(const Address& self, const Address& master, int max_listen_num) noexcept {
    TINYRPC_NS::Client c;
    co_await c.connect(master.host.c_str(), master.port);
    co_await TINYRPC_NS::call_func<void>(c, "register_map", self);

    auto& rpc = TINYRPC_NS::Server::get();
    TINYRPC_NS::register_func("map", map_func);
    rpc.init(self.host.c_str(), self.port, max_listen_num);
    co_await rpc.run();
}


int main(int argc, char** argv) {
    CLI::App app;
    int max_listen_num = 1024;
    Address self("127.0.0.1", 7777);
    Address master("127.0.0.1", 9999);

    argv = app.ensure_utf8(argv);
    app.add_option("--self-host", self.host, "self host");
    app.add_option("--self-port", self.port, "self port");
    app.add_option("--master-host", master.host, "master host");
    app.add_option("--master-port", master.port, "master port");
    app.add_option("-l,--listen-number", max_listen_num, "max listen number");
    CLI11_PARSE(app, argc, argv);

    ASYNCIO_NS::run(map_task(self, master, max_listen_num));
}
