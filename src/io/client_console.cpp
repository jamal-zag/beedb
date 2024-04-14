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

#include <io/client_console.h>
#include <io/result_output_formatter.h>
#include <io/server_response.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <regex>
#include <util/command_line_interface.h>

using namespace beedb::io;

#ifndef NDEBUG
constexpr static auto BEEDB_PROMPT = "beedb (DEBUG)> ";
#else
constexpr static auto BEEDB_PROMPT = "beedb> ";
#endif

ClientConsole::ClientConsole(std::string &&server, const std::uint16_t port) : _client(std::move(server), port)
{
}

void ClientConsole::run()
{
    if (this->_client.connect())
    {
        std::cout << "Connected to BeeDB server " << this->_client.server() << ":" << this->_client.port() << std::endl;
    }
    else
    {
        std::cerr << "Could not connect to BeeDB server " << this->_client.server() << ":" << this->_client.port()
                  << std::endl;
        return;
    }

    const auto quit_regex = std::regex("q|quit|:q|:quit|e|exit|:e|:exit", std::regex_constants::icase);
    std::cout << "Type 'q' or 'quit' to exit." << std::endl;

    auto cli = util::CommandLineInterface{Config::cli_history_file, BEEDB_PROMPT};

    auto user_input = std::optional<std::string>{};
    while ((user_input = cli.next()) != std::nullopt)
    {
        auto match = std::smatch{};
        if (std::regex_match(user_input.value(), match, quit_regex))
        {
            this->_client.disconnect();
            std::cout << "Bye." << std::endl;
            return;
        }
        else
        {
            const auto response = this->_client.send(user_input.value());
            if (ClientConsole::handle_response(response) == false)
            {
                return;
            }
        }
    }

    std::cout << "Client closed. Server may still run." << std::endl;
}

bool ClientConsole::handle_response(const std::string &response)
{
    const auto *server_response = reinterpret_cast<const ServerResponse *>(response.data());
    if (server_response->type() == ServerResponse::Type::Error)
    {
        const auto *error = reinterpret_cast<const ErrorResponse *>(server_response);
        std::cerr << "\033[0;31merror\033[0m> " << error->message() << std::endl;
    }
    else if (server_response->type() == ServerResponse::Type::AnalyticalResult)
    {
        const auto *records = reinterpret_cast<const AnalyticalResponse *>(server_response);
        const auto formatter = ResultOutputFormatter::from_serialized_data(records->count_rows(), records->data());
        std::cout << formatter << "Fetched \033[1;32m" << records->count_rows() << "\033[0m row"
                  << (records->count_rows() == 1U ? "" : "s") << " in \033[1;33m" << records->execution_time_in_ms()
                  << "\033[0m ms." << std::endl;
    }
    else if (server_response->type() == ServerResponse::Type::TransactionalResult)
    {
        // Please implement me!
    }
    else if (server_response->type() == ServerResponse::Type::PlanExplanation)
    {
        const auto *plan = reinterpret_cast<const QueryPlanResponse *>(server_response);
        auto plan_json = nlohmann::json::parse(plan->payload());
        util::TextTable text_Table;
        text_Table.header({"Operator", "Data", "Output"});
        ClientConsole::plan_to_table(text_Table, plan_json);
        std::cout << text_Table << std::flush;
    }
    else if (server_response->type() == ServerResponse::Type::ServerClosed)
    {
        std::cout << "BeeDB Server stopped." << std::endl;
        return false;
    }

    return true;
}

void ClientConsole::plan_to_table(util::TextTable &table, nlohmann::json &layer, std::uint16_t depth)
{
    auto name = std::string(depth, ' ') + layer["name"].get<std::string>();
    auto data = layer.contains("data") ? layer["data"].get<std::string>() : "";
    if (data.size() > 50)
    {
        data = data.substr(0, 50) + "...";
    }

    auto output = layer["output"].get<std::string>();
    if (output.size() > 50)
    {
        output = output.substr(0, 50) + "...";
    }

    table.push_back({std::move(name), std::move(data), std::move(output)});
    if (layer.contains("childs"))
    {
        for (auto &child : layer["childs"])
        {
            plan_to_table(table, child, depth + 2);
        }
    }
}