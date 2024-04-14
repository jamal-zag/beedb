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
#include "exception.h"
#include <string>

namespace beedb::exception
{
class ParserException : public DatabaseException
{
  public:
    explicit ParserException(const std::string &message) : DatabaseException(DatabaseException::Parser, message)
    {
    }
    ~ParserException() override = default;
};

class SqlException final : public ParserException
{
  public:
    SqlException(const std::string &parser_error, const std::size_t line, const std::size_t column)
        : ParserException(parser_error + " in line " + std::to_string(line) + ":" + std::to_string(column) + ".")
    {
    }

    ~SqlException() override = default;
};

class UnsupportedStatementException final : public ParserException
{
  public:
    explicit UnsupportedStatementException(const std::string &statement)
        : ParserException(statement + " is not supported.")
    {
    }

    ~UnsupportedStatementException() override = default;
};

class CanNotConvertNullptrException final : public ParserException
{
  public:
    CanNotConvertNullptrException() : ParserException("Can not convert nulltptr expression.")
    {
    }

    ~CanNotConvertNullptrException() override = default;
};

class UnsupportedOperatorException final : public ParserException
{
  public:
    explicit UnsupportedOperatorException(const std::string &operator_name)
        : ParserException("Unsupported predicate operator: " + operator_name + ".")
    {
    }

    ~UnsupportedOperatorException() override = default;
};

class UnsupportedColumnType final : public ParserException
{
  public:
    UnsupportedColumnType() : ParserException("Unsupported column type.")
    {
    }

    ~UnsupportedColumnType() override = default;
};
} // namespace beedb::exception