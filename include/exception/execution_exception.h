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
class ExecutionException : public DatabaseException
{
  public:
    explicit ExecutionException(const std::string &message) : DatabaseException(DatabaseException::Execution, message)
    {
    }
    ~ExecutionException() override = default;
};

class NoPhysicalOperatorForNode final : public ExecutionException
{
  public:
    explicit NoPhysicalOperatorForNode(const std::string &operator_name)
        : ExecutionException("Operator " + operator_name + " is not implemented physically.")
    {
    }

    ~NoPhysicalOperatorForNode() override = default;
};

class NotInTransactionException final : public ExecutionException
{
  public:
    NotInTransactionException() : ExecutionException("No active transaction. Please BEGIN TRANSACTION first.")
    {
    }

    ~NotInTransactionException() override = default;
};
} // namespace beedb::exception