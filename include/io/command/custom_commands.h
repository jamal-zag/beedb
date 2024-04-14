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
#include "custom_command_interface.h"
#include <network/server.h>

namespace beedb::io::command
{
class ShowCommand final : public CustomCommandInterface
{
  public:
    ShowCommand() = default;
    ~ShowCommand() override = default;

    [[nodiscard]] std::optional<ExecutionResult> execute(const std::string &parameters, Executor &executor,
                                                         ExecutionCallback &execution_callback) override;
    [[nodiscard]] std::string help() override
    {
        return std::string("Syntax> :show [tables,indices,columns]{1}");
    };
};

class ExplainCommand final : public CustomCommandInterface
{
  public:
    ExplainCommand() = default;
    ~ExplainCommand() override = default;
    [[nodiscard]] std::optional<ExecutionResult> execute(const std::string &input, Executor &executor,
                                                         ExecutionCallback &execution_callback) override;
    [[nodiscard]] std::string help() override
    {
        return std::string("Syntax> :explain <SQL-Statement>");
    };
};

class SetCommand final : public CustomCommandInterface
{
  public:
    explicit SetCommand(Config &config) : _config(config)
    {
    }
    ~SetCommand() override = default;

    [[nodiscard]] std::optional<ExecutionResult> execute(const std::string &input, Executor &executor,
                                                         ExecutionCallback &execution_callback) override;

    [[nodiscard]] std::string help() override
    {
        return std::string("Syntax> :set <Argument Name> <Numerical Value>");
    };

  private:
    Config &_config;
};

class GetCommand final : public CustomCommandInterface
{
  public:
    explicit GetCommand(Config &config) : _config(config)
    {
    }
    ~GetCommand() override = default;

    [[nodiscard]] std::optional<ExecutionResult> execute(const std::string &input, Executor &executor,
                                                         ExecutionCallback &execution_callback) override;

    [[nodiscard]] std::string help() override
    {
        return std::string("Syntax> :get [<Argument Name>]?");
    };

  private:
    Config &_config;
};

class StatsCommand final : public CustomCommandInterface
{
  public:
    explicit StatsCommand(Database &db) : _stats(db.system_statistics()), _db(db)
    {
    }
    ~StatsCommand() override = default;

    /**
     * @brief execute This triggers computation of statistics for a specified,
     * existing table. After confirming that the table exists,
     * computations are triggered.
     * @param input the table name
     * @return the plan that computes the statistics
     */
    [[nodiscard]] std::optional<ExecutionResult> execute(const std::string &input, Executor &executor,
                                                         ExecutionCallback &execution_callback) override;

    [[nodiscard]] std::string help() override
    {
        return std::string("Syntax> :stats <Table Name>");
    };

  private:
    statistic::SystemStatistics &_stats;
    Database &_db;
};

class StopServerCommand final : public CustomCommandInterface
{
  public:
    explicit StopServerCommand(network::Server *server) : _server(server)
    {
    }
    ~StopServerCommand() override = default;

    [[nodiscard]] std::optional<ExecutionResult> execute(const std::string &parameters, Executor &executor,
                                                         ExecutionCallback &execution_callback) override;
    [[nodiscard]] std::string help() override
    {
        return std::string("Syntax> :stop");
    }

  private:
    network::Server *_server;
};
} // namespace beedb::io::command
