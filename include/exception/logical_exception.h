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
class LogicalException : public DatabaseException
{
  public:
    explicit LogicalException(const std::string &message) : DatabaseException(DatabaseException::LogicalPlan, message)
    {
    }
    ~LogicalException() override = default;
};

class ColumnCanNotBeNull final : public LogicalException
{
  public:
    ColumnCanNotBeNull(const std::string &table_name, const std::string &column_name)
        : LogicalException("Value for column " + table_name + "." + column_name + " can not be NULL.")
    {
    }

    ~ColumnCanNotBeNull() override = default;
};

class ColumnNotFoundException final : public LogicalException
{
  public:
    ColumnNotFoundException(const std::string &table_name, const std::string &column_name)
        : ColumnNotFoundException(table_name + "." + column_name)
    {
    }

    explicit ColumnNotFoundException(const std::string &table_and_column_name)
        : LogicalException("Column " + table_and_column_name + " not found.")
    {
    }

    ~ColumnNotFoundException() override = default;
};

class ColumnNotGroupedException final : public LogicalException
{
  public:
    ColumnNotGroupedException(const std::string &table_name, const std::string &column_name)
        : LogicalException("Column " + table_name + "." + column_name + " is neither aggregated nor grouped.")
    {
    }

    ~ColumnNotGroupedException() override = default;
};

class ColumnNotIndexed final : public LogicalException
{
  public:
    ColumnNotIndexed(const std::string &table_name, const std::string &column_name)
        : LogicalException("Index for " + table_name + "." + column_name + " not found.")
    {
    }

    ~ColumnNotIndexed() override = default;
};

class IndexAlreadyExistsException final : public LogicalException
{
  public:
    IndexAlreadyExistsException(const std::string &table_name, const std::string &column_name)
        : LogicalException("Index for " + table_name + "." + column_name + " already exists.")
    {
    }

    ~IndexAlreadyExistsException() override = default;
};

class IndexUnsupportedTypeException final : public LogicalException
{
  public:
    explicit IndexUnsupportedTypeException(const std::string &type_name)
        : LogicalException("Type " + type_name + " can not be indexed, Type is unsupported.")
    {
    }

    ~IndexUnsupportedTypeException() override = default;
};

class MultipleGroupByException final : public LogicalException
{
  public:
    MultipleGroupByException() : LogicalException("Only one GROUP BY argument supported!")
    {
    }

    ~MultipleGroupByException() override = default;
};

class MultipleTableReferences final : public LogicalException
{
  public:
    explicit MultipleTableReferences(const std::string &table_name)
        : LogicalException("Multiple references to table " + table_name + " without disambiguation.")
    {
    }

    ~MultipleTableReferences() override = default;
};

class TableAlreadyExists final : public LogicalException
{
  public:
    explicit TableAlreadyExists(const std::string &table_name)
        : LogicalException("Table " + table_name + " already exists.")
    {
    }

    ~TableAlreadyExists() override = default;
};

class TableNotFoundException final : public LogicalException
{
  public:
    explicit TableNotFoundException(const std::string &table_name)
        : LogicalException("Table " + table_name + " not found.")
    {
    }

    TableNotFoundException(const std::string &table_name, const std::string &reference_name)
        : LogicalException("Can not resolve table reference " + table_name + " in statement " + reference_name + ".")
    {
    }

    ~TableNotFoundException() override = default;
};

class CanNotResolveColumnException final : public LogicalException
{
  public:
    CanNotResolveColumnException(const std::string &column_name, const std::string &statement)
        : LogicalException("Can not resolve attribute reference " + column_name + " to a table in statement " +
                           statement + ".")
    {
    }

    explicit CanNotResolveColumnException(const std::string &column_name)
        : LogicalException("Can not resolve attribute reference " + column_name + ".")
    {
    }

    ~CanNotResolveColumnException() override = default;
};

class NoUniqueReferenceException final : public LogicalException
{
  public:
    NoUniqueReferenceException(const std::string &attribute_name, const std::string &table1, const std::string &table2)
        : LogicalException("Can not uniquely reference attribute " + attribute_name + ". Both table " + table1 +
                           " and " + table2 + " include this attribute!")
    {
    }

    ~NoUniqueReferenceException() override = default;
};

class CanNotCreateTableException final : public LogicalException
{
  public:
    CanNotCreateTableException() : LogicalException("Can not create table.")
    {
    }

    ~CanNotCreateTableException() override = default;
};

class CanNotCreateIndexException final : public LogicalException
{
  public:
    CanNotCreateIndexException() : LogicalException("Can not create index.")
    {
    }

    ~CanNotCreateIndexException() override = default;
};

class CanNotInsertException final : public LogicalException
{
  public:
    CanNotInsertException() : LogicalException("Can not insert.")
    {
    }

    ~CanNotInsertException() override = default;
};
} // namespace beedb::exception