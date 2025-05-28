#include <filesystem>

#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>
#include <tinyrpc.hpp>

#include "types.hpp"
namespace fs = std::filesystem;


ASYNCIO_NS::Task<> async_main(const Address& master) noexcept {
    TINYRPC_NS::Client c;
    fs::path cwd(fs::path(__FILE__).parent_path().parent_path());
    std::vector<std::string> files {
        cwd / "pg-being_ernest.txt",
        cwd / "pg-dorian_gray.txt"
    };
    std::string out_dir = "/tmp";
    co_await c.connect(master.host.c_str(), master.port);
    co_await TINYRPC_NS::call_func<void>(c, "run", files, out_dir);
}


int main(int argc, char** argv) {
    CLI::App app;
    Address master("127.0.0.1", 9999);

    argv = app.ensure_utf8(argv);
    app.add_option("-i,--host", master.host, "master host");
    app.add_option("-p,--port", master.port, "master port");
    CLI11_PARSE(app, argc, argv);

    ASYNCIO_NS::run(async_main(master));
}
