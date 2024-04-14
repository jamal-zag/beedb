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

#include <database.h>
#include <exception/logical_exception.h>
#include <string>

namespace beedb::plan::logical
{
class TableReference
{
  public:
    TableReference(const std::string &name, const std::string &alias) : _table_name(name), _table_alias(alias)
    {
    }
    TableReference(TableReference &&) noexcept = default;
    TableReference(const TableReference &) = default;
    ~TableReference() = default;

    [[nodiscard]] const std::string &table_name() const
    {
        return _table_name;
    }
    [[nodiscard]] const std::string &table_alias() const
    {
        return _table_alias;
    }

    bool operator==(const std::string &name) const
    {
        return _table_name == name || _table_alias == name;
    }

  private:
    std::string _table_name;
    std::string _table_alias;
};

class TableMap
{
  public:
    TableMap() = default;
    ~TableMap() = default;

    void insert(table::Table *table, const std::string &table_alias)
    {
        _table_alias_to_name.insert(std::make_pair(table_alias, table->name()));

        for (const auto &term : table->schema().terms())
        {
            const auto &column_name = term.get<expression::Attribute>().column_name();
            if (_attribute_name_to_table_alias.find(column_name) == _attribute_name_to_table_alias.end())
            {
                _attribute_name_to_table_alias.insert(
                    std::make_pair(column_name, std::vector<std::string>{table_alias}));
            }
            else
            {
                _attribute_name_to_table_alias.at(column_name).push_back(table_alias);
            }
        }
    }

    void ensure_table_exists(const std::string &name)
    {
        if (_table_alias_to_name.find(name) == _table_alias_to_name.end())
        {
            throw exception::TableNotFoundException(name);
        }
    }

    void check_and_replace_table(expression::Attribute &attribute)
    {
        if (attribute.table_name().has_value())
        {
            ensure_table_exists(attribute.table_name().value());
        }
        else if (attribute.is_asterisk() == false)
        {
            if (_attribute_name_to_table_alias.find(attribute.column_name()) == _attribute_name_to_table_alias.end())
            {
                throw exception::CanNotResolveColumnException(attribute.column_name());
            }

            const auto &tables = _attribute_name_to_table_alias.at(attribute.column_name());
            if (tables.size() != 1)
            {
                throw exception::CanNotResolveColumnException(attribute.column_name());
            }

            attribute.table_name(tables.front());
        }
    }

    void check_and_replace_table(std::unique_ptr<expression::Operation> &operation)
    {
        expression::for_attribute(operation, [this](auto &attribute) { this->check_and_replace_table(attribute); });
    }

    std::vector<std::string> tables() const
    {
        std::vector<std::string> table_aliases;
        table_aliases.reserve(_table_alias_to_name.size());
        for (const auto &[_, name] : _table_alias_to_name)
        {
            table_aliases.push_back(name);
        }

        return table_aliases;
    }

  private:
    std::unordered_map<std::string, std::string> _table_alias_to_name;
    std::unordered_map<std::string, std::vector<std::string>> _attribute_name_to_table_alias;
};
} // namespace beedb::plan::logical