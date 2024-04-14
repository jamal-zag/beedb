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
#include <expression/operation.h>
#include <expression/term.h>
#include <memory>
#include <optional>
#include <string>
#include <table/schema.h>
#include <table/value.h>
#include <vector>
namespace beedb::parser
{

using Alias = std::optional<std::string>;
using TableDescr = std::pair<std::string, Alias>;

using JoinDescr = std::pair<TableDescr, std::unique_ptr<expression::Operation>>;

using WhereExpression = std::unique_ptr<expression::Operation>;

using GroupByExpression = std::vector<expression::Term>;

using OrderByItem = std::pair<std::unique_ptr<expression::Operation>, bool>;
using OrderByExpression = std::vector<OrderByItem>;

struct LimitExpression
{
    std::uint64_t limit;
    std::uint64_t offset;
};

class NodeInterface
{
  public:
    NodeInterface() noexcept = default;
    virtual ~NodeInterface() noexcept = default;
};

class CreateTableStatement final : public NodeInterface
{
  public:
    CreateTableStatement(std::string &&table_name, const bool if_not_exists, table::Schema &&schema) noexcept
        : _table_name(std::move(table_name)), _if_not_exists(if_not_exists), _schema(std::move(schema))
    {
    }
    ~CreateTableStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept
    {
        return _table_name;
    }
    [[nodiscard]] bool if_not_exists() const noexcept
    {
        return _if_not_exists;
    }
    [[nodiscard]] table::Schema &schema()
    {
        return _schema;
    }

  private:
    std::string _table_name;
    bool _if_not_exists;
    table::Schema _schema;
};

class CreateIndexStatement final : public NodeInterface
{
  public:
    CreateIndexStatement(std::string &&index_name, std::string &&table_name, std::string &&column_name,
                         const bool is_unique) noexcept
        : _index_name(std::move(index_name)), _table_name(std::move(table_name)), _column_name(std::move(column_name)),
          _is_unique(is_unique)
    {
    }
    ~CreateIndexStatement() noexcept override = default;

    [[nodiscard]] std::string &index_name() noexcept
    {
        return _index_name;
    }
    [[nodiscard]] std::string &table_name() noexcept
    {
        return _table_name;
    }
    [[nodiscard]] std::string &column_name() noexcept
    {
        return _column_name;
    }
    [[nodiscard]] bool is_unique() const noexcept
    {
        return _is_unique;
    }

  private:
    std::string _index_name;
    std::string _table_name;
    std::string _column_name;
    bool _is_unique;
};

class InsertStatement final : public NodeInterface
{
  public:
    InsertStatement(std::string &&table_name, std::vector<std::string> &&column_names,
                    std::vector<std::vector<table::Value>> &&values) noexcept
        : _table_name(std::move(table_name)), _column_names(std::move(column_names)), _values(std::move(values))
    {
    }
    ~InsertStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept
    {
        return _table_name;
    }
    [[nodiscard]] std::vector<std::string> &column_names() noexcept
    {
        return _column_names;
    }
    [[nodiscard]] std::vector<std::vector<table::Value>> &values() noexcept
    {
        return _values;
    }

  private:
    std::string _table_name;
    std::vector<std::string> _column_names;
    std::vector<std::vector<table::Value>> _values;
};

class UpdateStatement final : public NodeInterface
{
  public:
    UpdateStatement(std::string &&table_name,
                    std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>> &&updates,
                    WhereExpression &&where) noexcept
        : _table_name(std::move(table_name)), _updates(std::move(updates)), _where(std::move(where))
    {
    }
    ~UpdateStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept
    {
        return _table_name;
    }
    [[nodiscard]] std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>>
        &updates() noexcept
    {
        return _updates;
    }
    [[nodiscard]] WhereExpression &where() noexcept
    {
        return _where;
    }

  private:
    std::string _table_name;
    std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>> _updates;
    WhereExpression _where;
};

class DeleteStatement final : public NodeInterface
{
  public:
    DeleteStatement(std::string &&table_name, WhereExpression &&where) noexcept
        : _table_name(std::move(table_name)), _where(std::move(where))
    {
    }
    ~DeleteStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept
    {
        return _table_name;
    }
    [[nodiscard]] WhereExpression &where() noexcept
    {
        return _where;
    }

  private:
    std::string _table_name;
    WhereExpression _where;
};

class TransactionStatement final : public NodeInterface
{
  public:
    enum Type
    {
        BeginTransaction,
        CommitTransaction,
        AbortTransaction
    };

    explicit TransactionStatement(const Type type) : _type(type)
    {
    }

    ~TransactionStatement() override = default;

    [[nodiscard]] bool is_begin() const noexcept
    {
        return _type == Type::BeginTransaction;
    }
    [[nodiscard]] bool is_commit() const noexcept
    {
        return _type == Type::CommitTransaction;
    }
    [[nodiscard]] bool is_abort() const noexcept
    {
        return _type == Type::AbortTransaction;
    }

  private:
    const Type _type;
};

class SelectQuery final : public NodeInterface
{
  public:
    SelectQuery(std::vector<std::unique_ptr<expression::Operation>> &&attributes, std::vector<TableDescr> &&from,
                std::optional<std::vector<JoinDescr>> &&join, WhereExpression &&where,
                std::optional<GroupByExpression> &&group_by, std::optional<OrderByExpression> &&order_by,
                std::optional<LimitExpression> &&limit) noexcept
        : _attributes(std::move(attributes)), _from(std::move(from)), _join(std::move(join)), _where(std::move(where)),
          _group_by(std::move(group_by)), _order_by(std::move(order_by)), _limit(limit)
    {
    }

    ~SelectQuery() noexcept override = default;

    [[nodiscard]] std::vector<std::unique_ptr<expression::Operation>> &attributes() noexcept
    {
        return _attributes;
    }
    [[nodiscard]] std::vector<TableDescr> &from() noexcept
    {
        return _from;
    }
    [[nodiscard]] std::optional<std::vector<JoinDescr>> &join() noexcept
    {
        return _join;
    }
    [[nodiscard]] WhereExpression &where() noexcept
    {
        return _where;
    }
    [[nodiscard]] std::optional<GroupByExpression> &group_by() noexcept
    {
        return _group_by;
    }
    [[nodiscard]] std::optional<OrderByExpression> &order_by() noexcept
    {
        return _order_by;
    }
    [[nodiscard]] std::optional<LimitExpression> &limit() noexcept
    {
        return _limit;
    }

  private:
    std::vector<std::unique_ptr<expression::Operation>> _attributes;
    std::vector<TableDescr> _from;
    std::optional<std::vector<JoinDescr>> _join;
    WhereExpression _where;
    std::optional<GroupByExpression> _group_by;
    std::optional<OrderByExpression> _order_by;
    std::optional<LimitExpression> _limit;
};
} // namespace beedb::parser