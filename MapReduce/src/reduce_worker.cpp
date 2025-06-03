#include <fstream>
#include <filesystem>
#include <map>

#include <tinyrpc.hpp>
#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>

#include "types.hpp"
namespace fs = std::filesystem;


bool reduce_func(std::string in, std::string out) noexcept {
    if (!fs::exists(in)) {
        return false;
    }
    std::ifstream fin(in);
    std::ofstream fout(out);
    std::string word;
    std::map<std::string, size_t> stat;
    while (fin >> word) {
        ++stat[word];
    }
    for (auto& [word, count] : stat) {
        fout << word << ' ' << count << std::endl;
    }
    return true;
}


ASYNCIO_NS::Task<> reduce_task(
    const Address& self,
    const Address& master,
    int max_listen_num
) noexcept {
    TINYRPC_NS::Client c;
    co_await c.connect(master.host.c_str(), master.port);
    if (!co_await TINYRPC_NS::call_func<void>(c, "register_reduce", self)) {
        SPDLOG_ERROR("register reduce function failed");
        co_return;
    }

    TINYRPC_NS::Server rpc;
    TINYRPC_NS::register_func(rpc, "reduce", reduce_func);
    rpc.init(self.host.c_str(), self.port, max_listen_num);
    co_await rpc.run();
}


int main(int argc, char** argv) {
    CLI::App app;
    int max_listen_num = 1024;
    Address self("127.0.0.1", 8888);
    Address master("127.0.0.1", 9999);

    argv = app.ensure_utf8(argv);
    app.add_option("--self-host", self.host, "self host");
    app.add_option("--self-port", self.port, "self port");
    app.add_option("--master-host", master.host, "master host");
    app.add_option("--master-port", master.port, "master port");
    app.add_option("-l,--listen-number", max_listen_num, "max listen number");
    CLI11_PARSE(app, argc, argv);

    ASYNCIO_NS::run(reduce_task(self, master, max_listen_num));
}
