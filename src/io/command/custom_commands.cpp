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
#include <io/command/custom_commands.h>
#include <regex>

using namespace beedb::io::command;

std::optional<beedb::io::ExecutionResult> ShowCommand::execute(const std::string &parameters, Executor &executor,
                                                               ExecutionCallback &execution_callback)
{
    static const std::regex tables_regex("tables", std::regex::icase);
    static const std::regex columns_regex("columns", std::regex::icase);
    static const std::regex indices_regex("indices", std::regex::icase);
    std::smatch match;

    if (std::regex_match(parameters, match, tables_regex))
    {
        return executor.execute(Query{"select name as table_name from system_tables order by id asc;"},
                                execution_callback);
    }
    else if (std::regex_match(parameters, match, columns_regex))
    {
        Query q{"select t.name as table_name, c.name as column_name from system_columns c join system_tables t on "
                "c.table_id = t.id order by t.id asc, c.id asc;"};
        return executor.execute(q, execution_callback);
    }
    else if (std::regex_match(parameters, match, indices_regex))
    {
        Query q{"select t.name as table_name, c.name as column_name, i.name as index_name, i.is_unique as "
                "is_unique, i.type_id as type from system_indices i join system_columns c on i.column_id = c.id "
                "join system_tables t on c.table_id = t.id order by t.id asc, c.id asc, i.id asc;"};
        return executor.execute(q, execution_callback);
    }

    throw exception::CommandSyntaxException("Parameter is not recognized!", this->help());
}

std::optional<beedb::io::ExecutionResult> ExplainCommand::execute(const std::string &input, Executor &executor,
                                                                  ExecutionCallback &execution_callback)
{
    std::regex explain_regex("([plan]*)\\s(.*)", std::regex::icase);
    std::smatch explain_regex_match;

    if (std::regex_match(input, explain_regex_match, explain_regex))
    {
        if (explain_regex_match[1].str() == "plan")
        {
            Query q;
            return executor.execute(Query{explain_regex_match[2], Query::ExplainLevel::Plan}, execution_callback);
        }
        //        else if (explain_regex_match[1].str() == "graph")
        //        {
        //            return executor.execute(Query{explain_regex_match[2], Query::ExplainLevel::Graph},
        //            execution_callback);
        //        }
        else
        {
            throw exception::CommandSyntaxException("Unknown argument \'" + explain_regex_match[1].str() + "\'",
                                                    this->help());
        }
    }
    else if (input.empty())
    {
        throw exception::CommandSyntaxException("No query specified!", this->help());
    }

    return executor.execute(Query{input, Query::ExplainLevel::Plan}, execution_callback);
}

std::optional<beedb::io::ExecutionResult> SetCommand::execute(const std::string &input, Executor &, ExecutionCallback &)
{
    std::regex set_regex("(\\w+)\\s(.+)", std::regex::icase);
    std::regex value_regex("[0-9]+", std::regex::icase);
    std::smatch set_regex_match;
    std::smatch value_regex_match;

    if (std::regex_match(input, set_regex_match, set_regex))
    {
        const auto attribute_name = set_regex_match[1].str();
        const auto value = set_regex_match[2].str();

        if (!std::regex_match(value, value_regex_match, value_regex))
        {
            throw exception::CommandException("Options can only be set to numerical values!");
        }

        // Setting attribute to value....
        std::optional<Config::ConfigValue> old_value = std::nullopt;
        if (_config.contains(attribute_name))
        {
            old_value.emplace(_config[attribute_name]);
        }

        _config.set(attribute_name, std::stoi(value));

        std::cout << "Setting option \'" << attribute_name << "\' to value " << value;
        if (old_value.has_value())
        {
            std::cout << " (was " << old_value.value() << ")";
        }
        std::cout << std::endl;
    }
    else if (input.empty() == false)
    {
        std::regex option_regex("(\\w+)", std::regex::icase);
        // TODO: set the default option instead of throwing an error!
        throw exception::CommandSyntaxException("No value specified for option \'" + input + "\'", this->help());
    }
    else
    {
        throw exception::CommandSyntaxException("No arguments specified!", this->help());
    }
    return std::nullopt;
}

std::optional<beedb::io::ExecutionResult> GetCommand::execute(const std::string &input, Executor &, ExecutionCallback &)
{
    std::regex get_regex("(\\w+)");
    std::smatch get_regex_match;

    if (std::regex_match(input, get_regex_match, get_regex))
    {
        // we try to find the option by name..
        if (_config.contains(input))
        {
            std::cout << input << ": " << static_cast<std::int32_t>(_config[input]) << std::endl;
        }
        else
        {
            throw exception::CommandException("Option not found!");
        }
    }
    else if (input.empty())
    {
        // when no argument was specified, we print all options:
        std::cout << static_cast<std::string>(_config) << std::endl;
    }
    else
    {
        throw exception::CommandException("\'" + input + "\' is not a valid option name!");
    }
    return std::nullopt;
}

std::optional<beedb::io::ExecutionResult> StatsCommand::execute(const std::string &input, Executor &,
                                                                ExecutionCallback &)
{
    std::regex get_regex("(\\w+)");
    std::smatch get_regex_match;

    if (std::regex_match(input, get_regex_match, get_regex))
    {
        // Consistency check: we try to find the table by name..
        if (!_db.table_exists(input))
        {
            throw exception::CommandException("Table not found!");
        }

        // create execution plan, that performs statistic computations:
        // TODO: Need transaction here
        //        auto plan = plan::physical::Builder::build_statistics_plan(_db, input);
        //
        //        return _executor.execute(plan); // execute operator right away
    }
    else if (input.empty())
    {
        // when no argument was specified, this is an error:
        throw exception::CommandException("No table name specified!");
    }
    else
    {
        throw exception::CommandException("\'" + input + "\' is not a valid table name!");
    }
    return std::nullopt;
}

std::optional<beedb::io::ExecutionResult> StopServerCommand::execute(const std::string &, Executor &,
                                                                     ExecutionCallback &)
{
    if (this->_server != nullptr)
    {
        this->_server->stop();
    }

    return std::nullopt;
}
