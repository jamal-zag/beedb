/*------------------------------------------------------------------------------*
 * Architecture & Implementation of DBMS                                        *
 *------------------------------------------------------------------------------*
 * Copyright 2022 Databases and Information Systems Group TU Dortmund           *
 * Visit us at                                                                  *
 *             http://dbis.cs.tu-dortmund.de/cms/en/home/                       *
 *                                                                              *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS      *
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL      *
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR         *
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,        *
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR        *
 * OTHER DEALINGS IN THE SOFTWARE.                                              *
 *                                                                              *
 * Authors:                                                                     *
 *          Maximilian Berens   <maximilian.berens@tu-dortmund.de>              *
 *          Roland Kühn         <roland.kuehn@cs.tu-dortmund.de>                *
 *          Jan Mühlig          <jan.muehlig@tu-dortmund.de>                    *
 *------------------------------------------------------------------------------*
 */

#include <argparse/argparse.hpp>
#include <chrono>
#include <config.h>
#include <database.h>
#include <io/client_console.h>
#include <io/client_handler.h>
#include <io/command/commander.h>
#include <io/file_executor.h>
#include <io/result_output_formatter.h>
#include <network/server.h>
#include <string>
#include <thread>
#include <util/ini_parser.h>

int main(int arg_count, char **args)
{
    // Read configuration file.
    auto ini_parser = beedb::util::IniParser{"beedb.ini"};
    if (ini_parser.empty())
    {
        std::cout << "[Warning] Missing configuration file beedb.ini" << std::endl;
    }

    const auto buffer_frames = ini_parser.get<std::uint32_t>("buffer manager", "frames", 256u);
    const auto scan_page_limit = ini_parser.get<std::uint32_t>("scan", "page-limit", 64u);
    const auto buffer_replacement_strategy = ini_parser.get<std::string>("buffer manager", "strategy", "Random");
    const auto lru_k = ini_parser.get<std::uint32_t>("buffer manager", "k", 2u);
    const auto enable_index_scan = ini_parser.get<bool>("optimizer", "enable-index-scan", false);
    const auto enable_hash_join = ini_parser.get<bool>("optimizer", "enable-hash-join", false);
    const auto enable_predicate_push_down = ini_parser.get<bool>("optimizer", "enable-predicate-push-down", false);
    const auto print_statistics = ini_parser.get<bool>("executor", "print-statistics", false);

    // Parse command line arguments
    auto argument_parser = argparse::ArgumentParser{"beedb"};
    argument_parser.add_argument("db-file")
        .help("File the database is stored in. Default: bee.db")
        .default_value(std::string("bee.db"));
    argument_parser.add_argument("-p", "--port")
        .help("Port of the server")
        .default_value(std::uint16_t(4000))
        .action([](const std::string &value) { return std::uint16_t(std::stoul(value)); });
    argument_parser.add_argument("-l", "--load").help("Load SQL file into database.").default_value(std::string(""));
    argument_parser.add_argument("-q", "--query").help("Execute Query.").default_value(std::string(""));
    argument_parser.add_argument("-cmd", "--custom_command")
        .help("Execute custom command and exit right after.")
        .default_value(std::string(""));
    argument_parser.add_argument("-k", "--keep")
        .help("Keep server running after executing query, command or loading a file.")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("-c", "--client")
        .help("Start an additional client next to the server")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("--buffer-manager-frames")
        .help("Number of frames within the frame buffer.")
        .default_value(buffer_frames)
        .action([](const std::string &value) { return std::uint32_t(std::stoi(value)); });
    argument_parser.add_argument("--scan-page-limit")
        .help("Number of pages the SCAN operator can pin at a time.")
        .default_value(scan_page_limit)
        .action([](const std::string &value) { return std::uint32_t(std::stoi(value)); });
    argument_parser.add_argument("--enable-index-scan")
        .help("Enable index scan and use whenever possible.")
        .implicit_value(true)
        .default_value(enable_index_scan);
    argument_parser.add_argument("--enable-hash-join")
        .help("Enable hash join and use whenever possible.")
        .implicit_value(true)
        .default_value(enable_hash_join);
    argument_parser.add_argument("--enable-predicate-push-down")
        .help("Enable predicate push down and use whenever possible.")
        .implicit_value(true)
        .default_value(enable_predicate_push_down);
    argument_parser.add_argument("--stats")
        .help("Print all execution statistics")
        .implicit_value(true)
        .default_value(print_statistics);

    try
    {
        argument_parser.parse_args(arg_count, args);
    }
    catch (std::runtime_error &)
    {
        argument_parser.print_help();
        return 1;
    }

    const auto lru_regex = std::regex("lru", std::regex::icase);
    const auto lfu_regex = std::regex("lfu", std::regex::icase);
    const auto lru_k_regex = std::regex("lru-k", std::regex::icase);
    const auto clock_regex = std::regex("clock", std::regex::icase);
    auto match = std::smatch{};
    auto strategy = beedb::Config::Random;
    if (std::regex_match(buffer_replacement_strategy, match, lru_regex))
    {
        strategy = beedb::Config::LRU;
    }
    else if (std::regex_match(buffer_replacement_strategy, match, lfu_regex))
    {
        strategy = beedb::Config::LFU;
    }
    else if (std::regex_match(buffer_replacement_strategy, match, lru_k_regex))
    {
        strategy = beedb::Config::LRU_K;
    }
    else if (std::regex_match(buffer_replacement_strategy, match, clock_regex))
    {
        strategy = beedb::Config::Clock;
    }

    beedb::Config config{};
    config.set(beedb::Config::k_BufferFrames, argument_parser.get<std::uint32_t>("--buffer-manager-frames"),
               beedb::Config::ConfigMapValue::immutable);
    config.set(beedb::Config::k_BufferReplacementStrategy, strategy, beedb::Config::ConfigMapValue::immutable);
    config.set(beedb::Config::k_LRU_K, lru_k, beedb::Config::ConfigMapValue::immutable);
    config.set(beedb::Config::k_ScanPageLimit, argument_parser.get<std::uint32_t>("--scan-page-limit"),
               beedb::Config::ConfigMapValue::immutable);
    config.set(beedb::Config::k_OptimizationEnableIndexScan, argument_parser.get<bool>("--enable-index-scan"));
    config.set(beedb::Config::k_OptimizationEnableHashJoin, argument_parser.get<bool>("--enable-hash-join"));
    config.set(beedb::Config::k_OptimizationEnablePredicatePushDown,
               argument_parser.get<bool>("--enable-predicate-push-down"));
    config.set(beedb::Config::k_OptimizationDisableOptimization,
               true); // true (at first), to disable optimization during boot
    config.set(beedb::Config::k_CheckFinalPlan, false);
    config.set(beedb::Config::k_PrintExecutionStatistics, argument_parser.get<bool>("--stats"));

    const auto database_file_name = argument_parser.get<std::string>("db-file");
    const auto sql_file = argument_parser.get<std::string>("-l");
    const auto query = argument_parser.get<std::string>("-q");
    const auto keep = argument_parser.get<bool>("-k");
    const auto custom_command = argument_parser.get<std::string>("-cmd");

    auto database = beedb::Database{config, database_file_name};
    database.boot();

    // re-enable optimization for user-queries, after boot code ran:
    config.set(beedb::Config::k_OptimizationDisableOptimization, false);

#ifdef NDEBUG
    // re-enable checks on all plans, when in debug mode:
    config.set(beedb::Config::k_CheckFinalPlan, true);
#endif

    if (sql_file.empty() == false)
    {
        auto file_executor = beedb::io::FileExecutor{database};
        file_executor.execute(sql_file);
    }

    if (query.empty() == false)
    {
        auto result_formatter = beedb::io::ResultOutputFormatter{};
        auto executor = beedb::io::Executor{database, nullptr};
        auto result = executor.execute(beedb::io::Query{query}, result_formatter);
        if (result.error().empty() == false)
        {
            std::cerr << result.error() << std::endl;
        }
        else if (result_formatter.empty() == false)
        {
            std::cout << result_formatter << std::endl;
            std::cout << "Fetched \033[1;32m" << result.count_tuples() << "\033[0m row"
                      << (result.count_tuples() == 1U ? "" : "s") << " in \033[1;33m" << result.execution_time().count()
                      << "\033[0m ms." << std::endl;
        }
    }

    if (custom_command.empty() == false)
    {
        auto result_formatter = beedb::io::ResultOutputFormatter{};
        auto executor = beedb::io::Executor{database, nullptr};
        auto commander = beedb::io::command::Commander{database};
        auto result = commander.execute(custom_command, executor, result_formatter);

        if (result.has_value() && result->error().empty() == false)
        {
            std::cerr << result->error() << std::endl;
        }
        else if (result_formatter.empty() == false)
        {
            std::cout << result_formatter << std::endl;
        }

        return 0;
    }

    if ((sql_file.empty() == false || query.empty() == false || custom_command.empty() == false) && keep == false)
    {
        return 0;
    }

    const auto port = argument_parser.get<std::uint16_t>("-p");
    auto client_handler = beedb::io::ClientHandler{database};
    auto server = beedb::network::Server{client_handler, port};

    if (argument_parser.get<bool>("-c"))
    {
        auto client_thread = new std::thread([&port] {
            std::this_thread::sleep_for(std::chrono::milliseconds(400u));
            auto client_console = beedb::io::ClientConsole{"localhost", port};
            client_console.run();
        });
        client_thread->detach();
    }

    std::cout << "Starting beedb server on port " << server.port() << std::endl;
    const auto open = server.listen();
    if (open == false)
    {
        std::cerr << "Can not open server" << std::endl;
    }

    return 0;
}
