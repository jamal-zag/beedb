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

#include <exception/command_exception.h>
#include <io/client_handler.h>
#include <io/client_message_serializer.h>
#include <io/executor.h>
#include <io/query_result_serializer.h>
#include <io/server_response.h>

using namespace beedb::io;

ClientHandler::ClientHandler(Database &database) noexcept
    : _database(database), _commander(database), _client_transactions({nullptr})
{
    this->_client_transactions.fill(nullptr);
}

std::optional<std::string> ClientHandler::handle_message(const std::uint32_t client_id, const std::string &message)
{
    std::stringstream stream;
    auto executor = Executor{this->_database, this->_client_transactions[client_id]};
    auto result_serializer = ClientMessageSerializer{};

    if (command::Commander::is_command(message))
    {
        try
        {
            auto result = this->_commander.execute(message, executor, result_serializer);
            if (result.has_value())
            {
                if (result->is_successful() == false)
                {
                    return std::make_optional(ErrorResponse::build(result->error()));
                }
                else if (result_serializer.query_result().empty() == false)
                {
                    return std::make_optional(
                        AnalyticalResponse::build(result->build_time().count() + result->execution_time().count(),
                                                  result->count_tuples(), std::move(result_serializer.query_result())));
                }
                else if (result_serializer.query_plan().empty() == false)
                {
                    return std::make_optional(QueryPlanResponse::build(result_serializer.query_plan().dump()));
                }
                else
                {
                    return std::make_optional(EmptyResponse::build());
                }
            }
            else if (this->network::ClientHandler::server()->is_running() == false)
            {
                return std::make_optional(ServerClosedResponse::build());
            }
            else
            {
                return std::make_optional(EmptyResponse::build());
            }
        }
        catch (exception::CommandException &e)
        {
            return std::make_optional(ErrorResponse::build(e.what()));
        }
        catch (exception::DatabaseException &e)
        {
            return std::make_optional(ErrorResponse::build(e.what()));
        }
    }
    else
    {
        TransactionCallback transaction_callback{this->_client_transactions[client_id]};
        auto result = executor.execute(beedb::io::Query{message}, result_serializer, transaction_callback);

        if (result.is_successful())
        {
            if (result_serializer.query_result().empty() == false)
            {
                return std::make_optional(
                    AnalyticalResponse::build(result.build_time().count() + result.execution_time().count(),
                                              result.count_tuples(), std::move(result_serializer.query_result())));
            }
            else
            {
                return std::make_optional(EmptyResponse::build());
            }
        }
        else
        {
            return std::make_optional(ErrorResponse::build(result.error()));
        }
    }
}

void ClientHandler::on_client_connected(const std::uint32_t)
{
}

void ClientHandler::on_client_disconnected(const std::uint32_t client_id)
{
    if (this->_client_transactions[client_id] != nullptr)
    {
        this->_database.transaction_manager().abort(*this->_client_transactions[client_id]);
        this->_client_transactions[client_id] = nullptr;
    }
}