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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace beedb::io::command
{
class Commander
{
  public:
    explicit Commander(Database &database);
    ~Commander() = default;

    [[nodiscard]] static bool is_command(const std::string &input)
    {
        return input.at(0u) == ':';
    }

    void register_command(std::string &&name, std::unique_ptr<CustomCommandInterface> &&command)
    {
        _registered_commands.insert(std::make_pair(std::move(name), std::move(command)));
    }

    std::optional<beedb::io::ExecutionResult> execute(const std::string &user_input, Executor &executor,
                                                      ExecutionCallback &execution_callback);

  private:
    std::unordered_map<std::string, std::unique_ptr<CustomCommandInterface>> _registered_commands;
    Database &_database;
};
} // namespace beedb::io::command
