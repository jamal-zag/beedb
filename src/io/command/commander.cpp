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
#include <io/command/commander.h>
#include <io/command/custom_commands.h>
#include <regex>

using namespace beedb::io::command;

Commander::Commander(Database &database) : _database(database)
{
    this->register_command("show", std::make_unique<command::ShowCommand>());
    this->register_command("explain", std::make_unique<command::ExplainCommand>());
    this->register_command("set", std::make_unique<command::SetCommand>(this->_database.config()));
    this->register_command("get", std::make_unique<command::GetCommand>(this->_database.config()));
    this->register_command("stats", std::make_unique<command::StatsCommand>(this->_database));
}

std::optional<beedb::io::ExecutionResult> Commander::execute(const std::string &input, Executor &executor,
                                                             ExecutionCallback &execution_callback)
{
    std::regex command_regex(R"(:([\w\-]+)\s?(.*))", std::regex::icase);
    std::smatch match;

    if (std::regex_match(input, match, command_regex))
    {
        const auto name = match[1].str();
        const auto command_input = match[2].str();
        if (this->_registered_commands.find(name) != this->_registered_commands.end())
        {
            return this->_registered_commands.at(name)->execute(command_input, executor, execution_callback);
        }
        else
        {
            throw exception::UnknownCommandException(name);
        }
    }
    else
    {
        throw exception::UnknownCommandException(input.substr(1));
    }
}
