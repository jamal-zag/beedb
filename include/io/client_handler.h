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

#pragma once

#include <array>
#include <concurrency/transaction.h>
#include <concurrency/transaction_callback.h>
#include <config.h>
#include <database.h>
#include <io/command/commander.h>
#include <io/command/custom_commands.h>
#include <network/server.h>

namespace beedb::io
{
class ClientHandler : public network::ClientHandler
{
  public:
    ClientHandler(Database &database) noexcept;
    ~ClientHandler() noexcept override = default;
    std::optional<std::string> handle_message(const std::uint32_t client_id, const std::string &message) override;
    void on_client_connected(const std::uint32_t id) override;
    void on_client_disconnected(const std::uint32_t id) override;
    void server(network::Server *server) override
    {
        network::ClientHandler::server(server);
        _commander.register_command("stop", std::make_unique<command::StopServerCommand>(server));
    }

  private:
    Database &_database;
    command::Commander _commander;
    std::array<concurrency::Transaction *, Config::max_clients> _client_transactions;
};

class TransactionCallback : public concurrency::TransactionCallback
{
  public:
    TransactionCallback(concurrency::Transaction *&transaction) : _transaction(transaction)
    {
    }
    ~TransactionCallback() override = default;

    void on_begin(concurrency::Transaction *transaction) override
    {
        _transaction = transaction;
    }
    void on_end(concurrency::Transaction *, const bool) override
    {
        _transaction = nullptr;
    }

  private:
    concurrency::Transaction *&_transaction;
};

} // namespace beedb::io