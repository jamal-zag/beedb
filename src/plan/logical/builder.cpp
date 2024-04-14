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

#include <exception/logical_exception.h>
#include <plan/logical/builder.h>
#include <plan/logical/node/aggregation_node.h>
#include <plan/logical/node/arithmetic_node.h>
#include <plan/logical/node/create_index_node.h>
#include <plan/logical/node/create_table_node.h>
#include <plan/logical/node/cross_product_node.h>
#include <plan/logical/node/delete_node.h>
#include <plan/logical/node/insert_node.h>
#include <plan/logical/node/join_node.h>
#include <plan/logical/node/limit_node.h>
#include <plan/logical/node/order_by_node.h>
#include <plan/logical/node/projection_node.h>
#include <plan/logical/node/scan_node.h>
#include <plan/logical/node/selection_node.h>
#include <plan/logical/node/table.h>
#include <plan/logical/node/transaction_node.h>
#include <plan/logical/node/update_node.h>
#include <plan/optimizer/optimizer.h>

using namespace beedb::plan::logical;

std::unique_ptr<NodeInterface> Builder::build(Database &database, parser::SelectQuery *select_query)
{

    /// FILL THE PLAN:
    /// We build the canonical, 'straight-forward' plan:
    ///
    /// 0. Projections ("root" of the plan/the last operator)
    /// 1. Limit
    /// 2. OrderBy
    /// 3. Selections/filters
    /// 4. Aggregations
    /// 5. Arithmetics
    /// 6. Joins
    /// 7. Cross-products
    /// 8. Scan nodes

    /// (Step 1) Gather information from the parser:
    auto &select_part = select_query->attributes();
    auto &where_part = select_query->where();
    auto &group_by_part = select_query->group_by();
    auto &order_by_part = select_query->order_by();
    auto &limit_part = select_query->limit();

    /// (Step 2): Create simple nodes for the plan without any checks and optimizations.
    auto top_node = Builder::create_from(database, std::move(select_query->from()), std::move(select_query->join()));

    // Build projected terms, because selection will be destroyed by building algorithmic and aggregation.
    std::vector<std::unique_ptr<expression::Operation>> aggregations;
    std::vector<expression::Term> projection_terms;
    projection_terms.reserve(select_part.size());
    for (auto &select_operation : select_part)
    {
        top_node = Builder::parse_select_expression(std::move(top_node), projection_terms, aggregations,
                                                    std::move(select_operation), true);
    }

    // Where predicate.
    if (where_part != nullptr)
    {
        auto where_parts = Builder::split_logical_and(std::move(where_part));
        for (auto &&part : where_parts)
        {
            auto selection_node = std::make_unique<SelectionNode>(std::move(part));
            selection_node->child(std::move(top_node));
            top_node = std::move(selection_node);
        }
    }

    // Aggregation and/or group.
    if (group_by_part.has_value() || aggregations.empty() == false)
    {
        auto groups = group_by_part.value_or(std::vector<expression::Term>());
        auto aggregation_node = std::make_unique<AggregationNode>(std::move(aggregations), std::move(groups));
        aggregation_node->child(std::move(top_node));
        top_node = std::move(aggregation_node);
    }

    // Order by expressions.
    if (order_by_part.has_value())
    {
        auto order_by_node = std::make_unique<OrderByNode>(std::move(order_by_part.value()));
        order_by_node->child(std::move(top_node));
        top_node = std::move(order_by_node);
    }

    // Limit expression.
    if (limit_part.has_value())
    {
        auto limit_node = std::make_unique<LimitNode>(limit_part.value().limit, limit_part.value().offset);
        limit_node->child(std::move(top_node));
        top_node = std::move(limit_node);
    }

    // Projection
    auto projection_node = std::make_unique<ProjectionNode>(std::move(projection_terms));
    projection_node->child(std::move(top_node));
    top_node = std::move(projection_node);

    /// (Step 3): Create schemas and check attributes and tables.
    TableMap table_map;
    top_node->check_and_emit_schema(table_map);

    /// (Step 4): Do required optimizations/modifications to generate a physical plan.
    return RequiredOptimizer{database}(std::move(top_node));
}

std::unique_ptr<NodeInterface> Builder::create_from(Database &database, std::vector<parser::TableDescr> &&from,
                                                    std::optional<std::vector<parser::JoinDescr>> &&join)
{
    if (from.empty())
    {
        throw exception::LogicalException{"Missing FROM."};
    }

    if (join.has_value() && join->empty() == false)
    {
        /// (1) Build SCAN from "JOIN X" part.
        auto join_descriptor = std::move(join->back());
        auto &left_table_descriptor = std::get<0>(join_descriptor);
        auto &predicate = std::get<1>(join_descriptor);
        auto left_node = std::make_unique<TableScanNode>(
            database, TableReference{std::get<0>(left_table_descriptor),
                                     std::get<1>(left_table_descriptor).value_or(std::get<0>(left_table_descriptor))});

        /// (2) Build other FROM parts.
        join->pop_back();
        auto right_node = Builder::create_from(database, std::move(from), std::move(join));

        /// Join (1) and (2).
        auto join_node = std::make_unique<NestedLoopsJoinNode>(std::move(predicate));
        join_node->left_child(std::move(left_node));
        join_node->right_child(std::move(right_node));
        return join_node;
    }

    if (from.size() > 1U)
    {
        /// (1) Build SCAN from "FROM X,..." part.
        auto left_table_reference =
            TableReference{std::get<0>(from.front()), std::get<1>(from.front()).value_or(std::get<0>(from.front()))};
        auto left_node = std::make_unique<TableScanNode>(database, std::move(left_table_reference));

        /// (2) Build other FROM parts.
        from.erase(from.begin());
        auto right_node = Builder::create_from(database, std::move(from), std::nullopt);

        /// Cross (1) and (2).
        auto cross_product = std::make_unique<CrossProductNode>();
        cross_product->left_child(std::move(left_node));
        cross_product->right_child(std::move(right_node));
        return cross_product;
    }

    /// No joins, just a single "FROM".
    auto table_reference =
        TableReference{std::get<0>(from.front()), std::get<1>(from.front()).value_or(std::get<0>(from.front()))};
    return std::make_unique<TableScanNode>(database, std::move(table_reference));
}

std::unique_ptr<NodeInterface> Builder::parse_select_expression(
    std::unique_ptr<NodeInterface> &&top_node, std::vector<expression::Term> &projection_terms,
    std::vector<std::unique_ptr<expression::Operation>> &aggregations,
    std::unique_ptr<expression::Operation> &&select_expression, bool add_to_project)
{
    if (add_to_project && select_expression->result().has_value())
    {
        projection_terms.push_back(select_expression->result().value());
    }

    if (select_expression->is_arithmetic())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(select_expression.get());
        if (binary->left_child()->is_nullary() == false)
        {
            top_node = Builder::parse_select_expression(std::move(top_node), projection_terms, aggregations,
                                                        binary->left_child()->copy(), false);
        }

        if (binary->right_child()->is_nullary() == false)
        {
            top_node = Builder::parse_select_expression(std::move(top_node), projection_terms, aggregations,
                                                        binary->right_child()->copy(), false);
        }

        auto arithmetic_node = std::make_unique<ArithmeticNode>(std::move(select_expression));
        arithmetic_node->child(std::move(top_node));
        top_node = std::move(arithmetic_node);
    }
    else if (select_expression->is_aggregation())
    {
        auto *unary = reinterpret_cast<expression::UnaryOperation *>(select_expression.get());
        if (unary->child()->is_nullary() == false)
        {
            top_node = Builder::parse_select_expression(std::move(top_node), projection_terms, aggregations,
                                                        unary->child()->copy(), false);
        }

        aggregations.emplace_back(std::move(select_expression));
    }

    return std::move(top_node);
}

std::unique_ptr<NodeInterface> Builder::build(Database &database, parser::CreateTableStatement *create_statement)
{
    std::unique_ptr<NodeInterface> top_node{nullptr};
    auto &table_name = create_statement->table_name();

    const auto table_exists = database.table_exists(table_name);
    if (table_exists && create_statement->if_not_exists() == true)
    {
        return top_node;
    }

    top_node =
        std::make_unique<CreateTableNode>(database, std::move(table_name), std::move(create_statement->schema()));

    TableMap table_map;
    top_node->check_and_emit_schema(table_map);

    return top_node;
}

std::unique_ptr<NodeInterface> Builder::build(Database &database, parser::CreateIndexStatement *create_statement)
{
    std::unique_ptr<NodeInterface> top_node{nullptr};

    auto &index_name = create_statement->index_name();

    auto attribute =
        expression::Attribute{std::move(create_statement->table_name()), std::move(create_statement->column_name())};

    auto create_index_node = std::make_unique<CreateIndexNode>(database, std::move(attribute), std::move(index_name),
                                                               index::Type::BTree, create_statement->is_unique());

    TableMap tables;
    create_index_node->check_and_emit_schema(tables);

    auto *table = database.table(tables.tables().front());
    const auto column_index = table->schema().column_index(create_index_node->attribute().column_name());
    if (column_index.has_value())
    {
        const auto &column = table->schema().column(column_index.has_value());
        const auto index = column.index(create_index_node->index_name());
        if (index)
        {
            throw exception::IndexAlreadyExistsException(table->name(), create_index_node->attribute().column_name());
        }
    }

    top_node = std::move(create_index_node);

    return top_node;
}

std::unique_ptr<NodeInterface> Builder::build(Database &database, parser::InsertStatement *insert_statement)
{
    std::unique_ptr<NodeInterface> top_node{nullptr};

    top_node = std::make_unique<InsertNode>(database, std::move(insert_statement->table_name()),
                                            std::move(insert_statement->column_names()),
                                            std::move(insert_statement->values()));

    TableMap table_map;
    top_node->check_and_emit_schema(table_map);

    return top_node;
}

std::unique_ptr<NodeInterface> Builder::build(Database &database, parser::UpdateStatement *update_statement)
{
    auto &table_name = update_statement->table_name();

    const auto table_exists = database.table_exists(table_name);
    if (table_exists == false)
    {
        throw exception::TableNotFoundException{table_name};
    }

    std::unique_ptr<NodeInterface> top_node{nullptr};

    auto table_descriptor = std::vector<parser::TableDescr>{std::make_pair(std::move(table_name), std::nullopt)};
    top_node = Builder::create_from(database, std::move(table_descriptor), std::nullopt);

    if (update_statement->where() != nullptr)
    {
        auto selection_node = std::make_unique<SelectionNode>(std::move(update_statement->where()));
        selection_node->child(std::move(top_node));
        top_node = std::move(selection_node);
    }

    auto update_node = std::make_unique<UpdateNode>(std::move(update_statement->updates()));
    if (top_node != nullptr)
    {
        update_node->child(std::move(top_node));
    }

    top_node = std::move(update_node);

    TableMap tables;
    top_node->check_and_emit_schema(tables);

    return top_node;
}

std::unique_ptr<NodeInterface> Builder::build(Database &database, parser::DeleteStatement *delete_statement)
{
    auto &table_name = delete_statement->table_name();

    const auto table_exists = database.table_exists(table_name);
    if (table_exists == false)
    {
        throw exception::TableNotFoundException{table_name};
    }

    std::unique_ptr<NodeInterface> top_node{nullptr};

    auto table_descriptor = std::vector<parser::TableDescr>{std::make_pair(std::move(table_name), std::nullopt)};
    top_node = Builder::create_from(database, std::move(table_descriptor), std::nullopt);

    if (delete_statement->where() != nullptr)
    {
        auto selection_node = std::make_unique<SelectionNode>(std::move(delete_statement->where()));
        selection_node->child(std::move(top_node));
        top_node = std::move(selection_node);
    }

    auto delete_node = std::make_unique<DeleteNode>();
    if (top_node != nullptr)
    {
        delete_node->child(std::move(top_node));
    }

    top_node = std::move(delete_node);

    TableMap tables;
    top_node->check_and_emit_schema(tables);

    return top_node;
}

std::unique_ptr<NodeInterface> Builder::build(Database &, parser::TransactionStatement *transaction_statement)
{
    if (transaction_statement->is_begin())
    {
        return std::make_unique<BeginTransactionNode>();
    }
    else if (transaction_statement->is_commit())
    {
        return std::make_unique<CommitTransactionNode>();
    }
    else
    {
        return std::make_unique<AbortTransactionNode>();
    }
}

std::unique_ptr<NodeInterface> Builder::build(Database &database, const std::unique_ptr<parser::NodeInterface> &query)
{
    if (typeid(*query) == typeid(parser::SelectQuery))
    {
        return Builder::build(database, reinterpret_cast<parser::SelectQuery *>(query.get()));
    }

    if (typeid(*query) == typeid(parser::CreateTableStatement))
    {
        return Builder::build(database, reinterpret_cast<parser::CreateTableStatement *>(query.get()));
    }

    if (typeid(*query) == typeid(parser::CreateIndexStatement))
    {
        return Builder::build(database, reinterpret_cast<parser::CreateIndexStatement *>(query.get()));
    }

    if (typeid(*query) == typeid(parser::InsertStatement))
    {
        return Builder::build(database, reinterpret_cast<parser::InsertStatement *>(query.get()));
    }

    if (typeid(*query) == typeid(parser::UpdateStatement))
    {
        return Builder::build(database, reinterpret_cast<parser::UpdateStatement *>(query.get()));
    }

    if (typeid(*query) == typeid(parser::DeleteStatement))
    {
        return Builder::build(database, reinterpret_cast<parser::DeleteStatement *>(query.get()));
    }

    if (typeid(*query) == typeid(parser::TransactionStatement))
    {
        return Builder::build(database, reinterpret_cast<parser::TransactionStatement *>(query.get()));
    }

    throw exception::LogicalException("Logical plan construction not (yet) implemented for this query type.");
}

std::vector<std::unique_ptr<beedb::expression::Operation>> Builder::split_logical_and(
    std::unique_ptr<expression::Operation> &&root)
{
    std::vector<std::unique_ptr<expression::Operation>> parts;
    Builder::split_logical_and(std::move(root), parts);
    return parts;
}

void Builder::split_logical_and(std::unique_ptr<expression::Operation> &&root,
                                std::vector<std::unique_ptr<expression::Operation>> &container)
{
    if (root->type() == expression::Operation::And)
    {
        auto left = std::move(reinterpret_cast<expression::BinaryOperation *>(root.get())->left_child());
        split_logical_and(std::move(left), container);

        auto right = std::move(reinterpret_cast<expression::BinaryOperation *>(root.get())->right_child());
        split_logical_and(std::move(right), container);
    }
    else
    {
        container.emplace_back(std::move(root));
    }
}