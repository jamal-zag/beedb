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

#include <exception>
#include <string>

namespace beedb::exception
{
class CommandException : public std::exception
{
  public:
    explicit CommandException(const std::string &message) : _message(message)
    {
    }

    ~CommandException() override = default;

    [[nodiscard]] const char *what() const noexcept override
    {
        return _message.c_str();
    }

  private:
    const std::string _message;
};

class UnknownCommandException final : public CommandException
{
  public:
    explicit UnknownCommandException(const std::string &command)
        : CommandException("Unknown command '" + command + "'.")
    {
    }

    UnknownCommandException() : UnknownCommandException("Unknown command")
    {
    }

    ~UnknownCommandException() override = default;
};

class CommandSyntaxException final : public CommandException
{
  public:
    CommandSyntaxException(const std::string &command, const std::string &syntax_hint)
        : CommandException("Command syntax exception '" + command + "'.\n" + syntax_hint)
    {
    }

    CommandSyntaxException() : CommandException("Command syntax exception")
    {
    }

    ~CommandSyntaxException() override = default;
};

class UnknownCommandInputException final : public CommandException
{
  public:
    explicit UnknownCommandInputException(const std::string &input)
        : CommandException("Not supported input '" + input + "'")
    {
    }

    ~UnknownCommandInputException() override = default;
};
} // namespace beedb::exception
