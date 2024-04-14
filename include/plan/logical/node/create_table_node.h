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

#include "node_interface.h"

namespace beedb::plan::logical
{
class CreateTableNode final : public NotSchematizedNode
{
  public:
    CreateTableNode(Database &database, std::string &&table_name, table::Schema &&schema)
        : NotSchematizedNode("Create Table"), _database(database), _table_name(std::move(table_name)),
          _schema(std::move(schema))
    {
    }
    ~CreateTableNode() override = default;

    [[nodiscard]] const std::string &table_name() const
    {
        return _table_name;
    }

    [[nodiscard]] std::string &table_name()
    {
        return _table_name;
    }

    [[nodiscard]] const table::Schema &table_schema() const
    {
        return _schema;
    }
    [[nodiscard]] table::Schema &table_schema()
    {
        return _schema;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        if (_database.table_exists(_table_name))
        {
            throw exception::TableAlreadyExists(_table_name);
        }

        return NotSchematizedNode::check_and_emit_schema(tables);
    }

  private:
    Database &_database;
    std::string _table_name;
    table::Schema _schema;
};
} // namespace beedb::plan::logical