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

#include <linenoise.h>
#include <optional>
#include <string>

namespace beedb::util
{
class CommandLineInterface
{
  public:
    CommandLineInterface(std::string &&history_file_name, std::string &&prompt_message)
        : _history_file_name(std::move(history_file_name)), _prompt_message(std::move(prompt_message))
    {
        linenoiseHistoryLoad(_history_file_name.c_str());
    }

    std::optional<std::string> next()
    {
        char *line;
        if ((line = linenoise(_prompt_message.c_str())) != nullptr)
        {
            auto line_as_string = std::string{line};
            std::free(line);

            linenoiseHistoryAdd(line_as_string.c_str());
            linenoiseHistorySave(_history_file_name.c_str());

            return std::make_optional(std::move(line_as_string));
        }

        return std::nullopt;
    }

  private:
    const std::string _history_file_name;
    const std::string _prompt_message;
};
} // namespace beedb::util