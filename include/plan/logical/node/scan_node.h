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
#include "schema.h"
#include "table.h"
#include <database.h>

namespace beedb::plan::logical
{
class TableScanNode final : public NodeInterface
{
  public:
    TableScanNode(Database &database, TableReference &&table_reference)
        : NodeInterface("Sequential Scan"), _database(database), _table_reference(std::move(table_reference))
    {
    }
    ~TableScanNode() override = default;

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();

        if (_database.table_exists(_table_reference.table_name()) == false)
        {
            throw exception::TableNotFoundException(_table_reference.table_name());
        }

        auto *table = _database.table(_table_reference.table_name());

        tables.insert(table, _table_reference.table_alias());

        _schema.reserve(table->schema().size());
        for (const auto &term : table->schema().terms())
        {
            const auto &attribute_name = term.get<expression::Attribute>().column_name();
            _schema.emplace_back(
                expression::Term{expression::Attribute{_table_reference.table_alias(), attribute_name}});
        }
        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }
    [[nodiscard]] const TableReference &table() const
    {
        return _table_reference;
    }

    [[nodiscard]] nlohmann::json to_json() const override
    {
        auto json = NodeInterface::to_json();
        json["data"] = _table_reference.table_name();
        return json;
    }

  private:
    Database &_database;
    TableReference _table_reference;
    Schema _schema;
};

class IndexScanNode final : public NodeInterface
{
  public:
    IndexScanNode(Database &database, TableReference &&table_reference, expression::Attribute &&attribute,
                  std::unique_ptr<expression::Operation> &&predicate)
        : NodeInterface("Index Scan"), _database(database), _table_reference(std::move(table_reference)),
          _attribute(std::move(attribute)), _predicate(std::move(predicate))
    {
    }
    ~IndexScanNode() override = default;

    const Schema &check_and_emit_schema(TableMap &tables) override
    {
        _schema.clear();
        if (_database.table_exists(_table_reference.table_name()) == false)
        {
            throw exception::TableNotFoundException(_table_reference.table_name());
        }

        auto *table = _database.table(_table_reference.table_name());
        tables.insert(table, _table_reference.table_name());

        tables.check_and_replace_table(_predicate);
        tables.check_and_replace_table(_attribute);

        _schema.reserve(table->schema().size());
        for (const auto &term : table->schema().terms())
        {
            const auto &attribute_name = term.get<expression::Attribute>().column_name();
            _schema.emplace_back(
                expression::Term{expression::Attribute{_table_reference.table_alias(), attribute_name}});
        }
        return _schema;
    }

    [[nodiscard]] const Schema &schema() const override
    {
        return _schema;
    }
    [[nodiscard]] const TableReference &table() const
    {
        return _table_reference;
    }
    [[nodiscard]] const std::unique_ptr<expression::Operation> &predicate() const
    {
        return _predicate;
    }
    [[nodiscard]] const expression::Attribute &attribute() const
    {
        return _attribute;
    }

    void schema(const Schema &schema)
    {
        _schema.clear();
        std::copy(schema.begin(), schema.end(), std::back_inserter(_schema));
    }

  private:
    Database &_database;
    TableReference _table_reference;
    expression::Attribute _attribute;
    std::unique_ptr<expression::Operation> _predicate;
    Schema _schema;
};
} // namespace beedb::plan::logical