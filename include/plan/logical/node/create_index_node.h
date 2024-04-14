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
#include <index/type.h>

namespace beedb::plan::logical
{
class CreateIndexNode final : public NotSchematizedNode
{
  public:
    CreateIndexNode(Database &database, expression::Attribute &&attribute, std::string &&index_name,
                    const index::Type index_type, const bool is_unique)
        : NotSchematizedNode("Create Index"), _database(database), _attribute(std::move(attribute)),
          _index_name(std::move(index_name)), _index_type(index_type), _is_unique(is_unique)
    {
    }
    ~CreateIndexNode() override = default;

    [[nodiscard]] const expression::Attribute &attribute() const
    {
        return _attribute;
    }
    [[nodiscard]] const std::string &index_name() const
    {
        return _index_name;
    }
    [[nodiscard]] bool is_unique() const
    {
        return _is_unique;
    }
    [[nodiscard]] index::Type index_type() const
    {
        return _index_type;
    }

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        if (_database.table_exists(_attribute.table_name().value()) == false)
        {
            throw exception::TableNotFoundException(_attribute.table_name().value());
        }

        tables.insert(_database.table(_attribute.table_name().value()), _attribute.table_name().value());

        tables.check_and_replace_table(_attribute);

        return NotSchematizedNode::check_and_emit_schema(tables);
    }

  private:
    Database &_database;
    expression::Attribute _attribute;
    std::string _index_name;
    index::Type _index_type;
    bool _is_unique;
};
} // namespace beedb::plan::logical