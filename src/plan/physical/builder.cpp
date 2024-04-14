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

#include <exception/execution_exception.h>
#include <execution/add_to_index_operator.h>
#include <execution/aggregate_operator.h>
#include <execution/arithmetic_operator.h>
#include <execution/build_index_operator.h>
#include <execution/create_index_operator.h>
#include <execution/create_table_operator.h>
#include <execution/cross_product_operator.h>
#include <execution/delete_operator.h>
#include <execution/hash_join_operator.h>
#include <execution/index_scan_operator.h>
#include <execution/insert_operator.h>
#include <execution/limit_operator.h>
#include <execution/nested_loops_join_operator.h>
#include <execution/order_operator.h>
#include <execution/projection_operator.h>
#include <execution/selection_operator.h>
#include <execution/sequential_scan_operator.h>
#include <execution/transaction_operator.h>
#include <execution/tuple_buffer_operator.h>
#include <execution/update_operator.h>
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
#include <plan/logical/node/transaction_node.h>
#include <plan/logical/node/update_node.h>
#include <plan/physical/builder.h>
#include <type_traits>
#include <unordered_set>

using namespace beedb::plan::physical;

Plan Builder::build(Database &database, concurrency::Transaction *transaction,
                    concurrency::TransactionCallback &transaction_callback, bool add_to_scan_set,
                    const std::unique_ptr<logical::NodeInterface> &logical_plan)
{
    if (logical_plan == nullptr)
    {
        return Plan(database, nullptr);
    }

    auto execution_operator =
        Builder::build_operator(database, transaction, transaction_callback, add_to_scan_set, {}, logical_plan);
    return Plan(database, std::move(execution_operator));
}

std::unique_ptr<beedb::execution::OperatorInterface> Builder::build_operator(
    Database &database, concurrency::Transaction *transaction, concurrency::TransactionCallback &transaction_callback,
    bool add_to_scan_set, concurrency::ScanSetItem *scan_set,
    const std::unique_ptr<logical::NodeInterface> &logical_plan)
{
    if (logical_plan == nullptr)
    {
        return nullptr;
    }

    auto *logical_node = logical_plan.get();

    if (typeid(*logical_node) == typeid(logical::TableScanNode))
    {
        auto *table_node = reinterpret_cast<logical::TableScanNode *>(logical_node);
        auto *table = database.table(table_node->table().table_name());

        auto schema = table::Schema{table->schema(), table_node->schema()};

        if (add_to_scan_set)
        {
            if (scan_set == nullptr)
            {
                scan_set = new concurrency::ScanSetItem(*table);
            }
            else
            {
                scan_set->table(*table);
            }

            transaction->add_to_scan_set(scan_set);
        }

        return std::make_unique<execution::SequentialScanOperator>(
            transaction, static_cast<std::uint32_t>(database.config()[Config::k_ScanPageLimit]), std::move(schema),
            database.buffer_manager(), database.table_disk_manager(), *table);
    }
    else if (typeid(*logical_node) == typeid(logical::IndexScanNode))
    {
        auto *index_scan_node = reinterpret_cast<logical::IndexScanNode *>(logical_node);
        auto table = database.table(index_scan_node->table().table_name());
        auto schema = table::Schema{table->schema(), index_scan_node->schema()};

        auto key_ranges = Builder::extract_key_ranges(index_scan_node->predicate());
        bool needs_range = std::find_if(key_ranges.cbegin(), key_ranges.cend(), [](const auto &key_range) {
                               return key_range.single_key() == false;
                           }) != key_ranges.cend();

        const auto indexed_column_name = index_scan_node->attribute().column_name();
        const auto indexed_column_index = schema.column_index(indexed_column_name);
        assert(indexed_column_index.has_value());
        auto index = schema.column(indexed_column_index.value()).index(needs_range);
        if (add_to_scan_set)
        {
            if (scan_set == nullptr)
            {
                scan_set = new concurrency::ScanSetItem(*table);
            }
            else
            {
                scan_set->table(*table);
            }
            transaction->add_to_scan_set(scan_set);
        }

        return std::make_unique<execution::IndexScanOperator>(
            transaction, static_cast<std::uint32_t>(database.config()[Config::k_ScanPageLimit]), schema,
            database.buffer_manager(), database.table_disk_manager(), std::move(key_ranges), index);
    }
    else if (typeid(*logical_node) == typeid(logical::ProjectionNode))
    {
        auto *projection_node = reinterpret_cast<logical::ProjectionNode *>(logical_node);
        auto child = build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                    projection_node->child());

        auto schema = table::Schema{child->schema(), projection_node->schema()};
        auto projection_operator = std::make_unique<execution::ProjectionOperator>(transaction, std::move(schema));
        projection_operator->child(std::move(child));
        return projection_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::SelectionNode))
    {
        auto *selection_node = reinterpret_cast<logical::SelectionNode *>(logical_node);

        concurrency::ScanSetItem *scan_set_item = nullptr;
        if (add_to_scan_set)
        {
            scan_set_item = new concurrency::ScanSetItem();
        }

        auto child = Builder::build_operator(database, transaction, transaction_callback, add_to_scan_set,
                                             scan_set_item, selection_node->child());
        auto predicate_matcher = Builder::build_predicate(selection_node->predicate(), child->schema());
        if (scan_set_item != nullptr)
        {
            scan_set_item->predicate(predicate_matcher->clone());
        }

        auto selection_operator =
            std::make_unique<execution::SelectionOperator>(transaction, child->schema(), std::move(predicate_matcher));
        selection_operator->child(std::move(child));
        return selection_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::NestedLoopsJoinNode) ||
             typeid(*logical_node) == typeid(logical::HashJoinNode))
    {
        auto *join_node = reinterpret_cast<logical::AbstractJoinNode *>(logical_node);
        auto left_child = build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                         join_node->left_child());
        auto right_child = build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                          join_node->right_child());
        auto join_schema =
            table::Schema(left_child->schema(), right_child->schema(),
                          left_child->schema().table_name() + "_JOIN_" + right_child->schema().table_name());

        auto predicate_matcher = build_predicate(join_node->predicate(), left_child->schema(), right_child->schema());
        assert(predicate_matcher != nullptr && "Can not build predicate from join predicate.");
        std::unique_ptr<execution::BinaryOperator> join_operator{};
        if (typeid(*logical_node) == typeid(logical::HashJoinNode))
        {
            auto attribute_matcher =
                reinterpret_cast<execution::AttributeMatcher<execution::PredicateMatcherInterface::EQ> *>(
                    predicate_matcher.get());
            join_operator = std::make_unique<execution::HashJoinOperator>(
                transaction, std::move(join_schema), attribute_matcher->left_index(), attribute_matcher->right_index());
        }
        else
        {
            join_operator = std::make_unique<execution::NestedLoopsJoinOperator>(transaction, std::move(join_schema),
                                                                                 std::move(predicate_matcher));
        }

        join_operator->left_child(std::move(left_child));
        join_operator->right_child(std::move(right_child));
        return join_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::CrossProductNode))
    {
        auto *cross_product_node = reinterpret_cast<logical::CrossProductNode *>(logical_node);
        auto left_child = build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                         cross_product_node->left_child());
        auto right_child = build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                          cross_product_node->right_child());
        auto schema = table::Schema(left_child->schema(), right_child->schema(),
                                    left_child->schema().table_name() + "_CROSS_" + right_child->schema().table_name());
        auto cross_product_operator = std::make_unique<execution::CrossProductOperator>(transaction, std::move(schema));
        cross_product_operator->left_child(std::move(left_child));
        cross_product_operator->right_child(std::move(right_child));
        return cross_product_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::AggregationNode))
    {
        auto *group_by_node = reinterpret_cast<logical::AggregationNode *>(logical_node);
        auto child = build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                    group_by_node->child());

        std::vector<expression::Attribute> group_attributes;
        const auto &child_schema = child->schema();

        // Aggregator: Term-Name (e.g. COUNT(id)) => Physical aggregator.
        std::unordered_map<expression::Term, std::unique_ptr<execution::AggregatorInterface>> term_aggregators;
        term_aggregators.reserve(group_by_node->aggregation_expressions().size());
        std::unordered_map<expression::Term, table::Type::Id> aggregation_type;
        aggregation_type.reserve(group_by_node->aggregation_expressions().size());
        for (const auto &aggregation_expression : group_by_node->aggregation_expressions())
        {
            const auto child_term =
                reinterpret_cast<expression::UnaryOperation *>(aggregation_expression.get())->child()->result().value();
            const auto aggregation_result = aggregation_expression->result().value();
            const auto is_count_asterisk = aggregation_expression->type() == expression::Operation::Type::Count &&
                                           child_term.is_attribute() &&
                                           child_term.get<expression::Attribute>().is_asterisk();

            if (is_count_asterisk)
            {
                aggregation_type.insert(std::make_pair(aggregation_result, table::Type::Id::LONG));
                term_aggregators.insert(std::make_pair(
                    aggregation_result,
                    std::make_unique<execution::CountAggregator>(std::numeric_limits<std::uint16_t>::max())));
            }
            else
            {
                const auto child_index =
                    child_schema.column_index(child_term.get<expression::Attribute>().column_name());
                if (child_index.has_value() == false)
                {
                    throw exception::ExecutionException("Can not aggregate.");
                }

                const auto index = child_index.value();

                if (aggregation_expression->type() == expression::Operation::Type::Count)
                {
                    aggregation_type.insert(std::make_pair(aggregation_result, table::Type::Id::LONG));
                    term_aggregators.insert(
                        std::make_pair(aggregation_result, std::make_unique<execution::CountAggregator>(index)));
                }
                else if (aggregation_expression->type() == expression::Operation::Type::Average)
                {
                    const auto type = child_schema.column(index).type();
                    aggregation_type.insert(std::make_pair(aggregation_result, table::Type::Id::DECIMAL));
                    term_aggregators.insert(std::make_pair(
                        aggregation_result, std::make_unique<execution::AverageAggregator>(index, type)));
                }
                else if (aggregation_expression->type() == expression::Operation::Type::Sum)
                {
                    const auto type = child_schema.column(index).type();
                    aggregation_type.insert(std::make_pair(aggregation_result, type));
                    term_aggregators.insert(
                        std::make_pair(aggregation_result, std::make_unique<execution::SumAggregator>(index, type)));
                }
                else if (aggregation_expression->type() == expression::Operation::Type::Min)
                {
                    const auto type = child_schema.column(index).type();
                    aggregation_type.insert(std::make_pair(aggregation_result, type));
                    term_aggregators.insert(
                        std::make_pair(aggregation_result, std::make_unique<execution::MinAggregator>(index, type)));
                }
                else if (aggregation_expression->type() == expression::Operation::Type::Max)
                {
                    const auto type = child_schema.column(index).type();
                    aggregation_type.insert(std::make_pair(aggregation_result, type));
                    term_aggregators.insert(
                        std::make_pair(aggregation_result, std::make_unique<execution::MaxAggregator>(index, type)));
                }
            }
        }

        const auto &node_schema = group_by_node->schema();
        table::Schema schema{"AGGREGATION"};
        for (const auto &term : node_schema)
        {
            const auto child_schema_index = child_schema.column_index(term);
            if (child_schema_index.has_value())
            {
                schema.add(table::Column{child_schema.column(child_schema_index.value())}, expression::Term{term});
            }
            else if (aggregation_type.find(term) != aggregation_type.end())
            {
                schema.add(table::Column{aggregation_type.at(term)}, expression::Term{term});
            }
        }

        std::vector<std::pair<std::uint16_t, std::uint16_t>> groups;
        groups.reserve(group_by_node->group_expressions().size());
        for (const auto &group : group_by_node->group_expressions())
        {
            const auto child_index = child_schema.column_index(group);
            if (child_index.has_value())
            {
                const auto index = schema.column_index(group);
                if (index.has_value())
                {
                    groups.emplace_back(std::make_pair(child_index.value(), index.value()));
                }
            }
        }

        auto group_by_operator = std::make_unique<execution::AggregateOperator>(
            transaction, std::move(schema), std::move(groups), std::move(term_aggregators));
        group_by_operator->child(std::move(child));
        return group_by_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::ArithmeticNode))
    {
        auto *arithmetic_node = reinterpret_cast<logical::ArithmeticNode *>(logical_node);
        auto child = Builder::build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                             arithmetic_node->child());

        auto [type, calculator] = Builder::build_arithmetic_calculator(arithmetic_node->expression(), child->schema());
        auto schema = table::Schema{"ARITHMETIC"};
        for (const auto &term : arithmetic_node->schema())
        {
            const auto child_index = child->schema().column_index(term);
            if (child_index.has_value())
            {
                schema.add(table::Column{child->schema().column(child_index.value())}, expression::Term{term});
            }
            else if (term == arithmetic_node->expression()->result().value())
            {
                schema.add(table::Column{type}, expression::Term{term});
            }
        }

        std::unordered_map<std::uint16_t, std::unique_ptr<execution::ArithmeticCalculatorInterface>> calculators;
        calculators.insert(std::make_pair(schema.column_index(arithmetic_node->expression()->result().value()).value(),
                                          std::move(calculator)));

        auto arithmetic_operator =
            std::make_unique<execution::ArithmeticOperator>(transaction, std::move(schema), std::move(calculators));
        arithmetic_operator->child(std::move(child));
        return arithmetic_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::OrderByNode))
    {
        auto *order_by_node = reinterpret_cast<logical::OrderByNode *>(logical_node);
        auto child = Builder::build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                             order_by_node->child());
        std::vector<std::pair<std::uint32_t, bool>> sort_indices;
        for (const auto &operation_and_direction : order_by_node->predicates())
        {
            const auto &order_operation = std::get<0>(operation_and_direction);
            if (order_operation->result().has_value())
            {
                const auto is_asc = std::get<1>(operation_and_direction);
                auto index = child->schema().column_index(order_operation->result().value());
                if (index.has_value())
                {
                    sort_indices.emplace_back(std::make_pair(index.value(), is_asc));
                }
            }
        }
        auto order_by_operator =
            std::make_unique<execution::OrderOperator>(transaction, child->schema(), std::move(sort_indices));
        order_by_operator->child(std::move(child));
        return order_by_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::LimitNode))
    {
        auto *limit_node = reinterpret_cast<logical::LimitNode *>(logical_node);
        auto child =
            build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr, limit_node->child());
        auto limit_operator = std::make_unique<execution::LimitOperator>(transaction, child->schema(),
                                                                         limit_node->limit(), limit_node->offset());
        limit_operator->child(std::move(child));
        return limit_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::CreateTableNode))
    {
        auto *create_table_node = reinterpret_cast<logical::CreateTableNode *>(logical_node);

        auto schema =
            table::Schema{std::move(create_table_node->table_name()), std::move(create_table_node->table_schema())};

        return std::make_unique<execution::CreateTableOperator>(database, transaction, std::move(schema));
    }
    else if (typeid(*logical_node) == typeid(logical::CreateIndexNode))
    {
        auto *create_index_node = reinterpret_cast<logical::CreateIndexNode *>(logical_node);

        const auto &table_name = create_index_node->attribute().table_name().value();
        auto create_index_operator = std::make_unique<execution::CreateIndexOperator>(
            database, transaction, table_name, create_index_node->attribute().column_name(),
            create_index_node->index_name(), create_index_node->is_unique(), create_index_node->index_type());

        auto table = database.table(table_name);
        auto schema = table->schema();
        auto scan_operator = std::make_unique<execution::SequentialScanOperator>(
            transaction, static_cast<std::uint32_t>(database.config()[Config::k_ScanPageLimit]), std::move(schema),
            database.buffer_manager(), database.table_disk_manager(), *table);
        auto column_index = scan_operator->schema().column_index(create_index_node->attribute().column_name());
        assert(column_index.has_value());

        auto build_index_operator = std::make_unique<execution::BuildIndexOperator>(
            database, transaction, table_name, column_index.value(), create_index_node->index_name());
        build_index_operator->create_index_operator(std::move(create_index_operator));
        build_index_operator->data_operator(std::move(scan_operator));

        return build_index_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::InsertNode))
    {
        auto *insert_node = reinterpret_cast<logical::InsertNode *>(logical_node);
        auto *table = database.table(insert_node->table_name());
        auto tuple_buffer_operator = std::make_unique<execution::TupleBufferOperator>(transaction, table->schema());

        std::vector<table::Schema::ColumnIndexType> column_indices;
        column_indices.reserve(insert_node->column_names().size());
        for (const auto &column_name : insert_node->column_names())
        {
            column_indices.push_back(table->schema().column_index(column_name).value());
        }

        std::vector<table::Schema::ColumnIndexType> default_column_indices;
        default_column_indices.reserve(table->schema().size() - column_indices.size());
        for (auto i = 0u; i < table->schema().size(); ++i)
        {
            if (std::find(column_indices.begin(), column_indices.end(), i) == column_indices.end())
            {
                default_column_indices.push_back(i);
            }
        }

        for (auto &values : insert_node->value_lists())
        {
            auto tuple =
                Builder::build_tuple(tuple_buffer_operator->schema(), column_indices, default_column_indices, values);
            tuple_buffer_operator->add(tuple);
        }

        auto insert_operator = std::unique_ptr<execution::UnaryOperator>(
            new execution::InsertOperator(transaction, database.buffer_manager(), database.table_disk_manager(),
                                          database.system_statistics(), *table));
        insert_operator->child(std::move(tuple_buffer_operator));

        for (auto i = 0u; i < table->schema().size(); i++)
        {
            const auto &column = table->schema().column(i);
            for (auto &index : column.indices())
            {
                auto add_to_index_operator =
                    std::unique_ptr<execution::UnaryOperator>(new execution::AddToIndexOperator(transaction, i, index));
                add_to_index_operator->child(std::move(insert_operator));
                insert_operator = std::move(add_to_index_operator);
            }
        }

        return insert_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::UpdateNode))
    {
        auto *update_node = reinterpret_cast<logical::UpdateNode *>(logical_node);
        auto child =
            build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr, update_node->child());

        std::vector<std::pair<table::Schema::ColumnIndexType, table::Value>> new_column_values;
        new_column_values.reserve(update_node->updates().size());
        for (const auto &update : update_node->updates())
        {
            const auto term = expression::Term{std::get<0>(update)};
            const auto index = child->schema().column_index(term);
            if (index.has_value() && std::get<1>(update)->result().has_value())
            {
                new_column_values.emplace_back(
                    std::make_pair(index.value(), Builder::build_value(std::get<1>(update)->result().value(),
                                                                       child->schema().column(index.value()).type())));
            }
        }

        auto update_operator = std::make_unique<execution::UpdateOperator>(
            transaction, *database[update_node->table_name()], database.table_disk_manager(), database.buffer_manager(),
            std::move(new_column_values));
        update_operator->child(std::move(child));
        return update_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::DeleteNode))
    {
        auto *delete_node = reinterpret_cast<logical::DeleteNode *>(logical_node);
        auto child = Builder::build_operator(database, transaction, transaction_callback, add_to_scan_set, nullptr,
                                             delete_node->child());

        auto delete_operator =
            std::make_unique<execution::DeleteOperator>(transaction, *database[delete_node->table_name()],
                                                        database.table_disk_manager(), database.buffer_manager());
        delete_operator->child(std::move(child));
        return delete_operator;
    }
    else if (typeid(*logical_node) == typeid(logical::BeginTransactionNode))
    {
        return std::make_unique<execution::BeginTransactionOperator>(database.transaction_manager(),
                                                                     transaction_callback);
    }
    else if (typeid(*logical_node) == typeid(logical::AbortTransactionNode))
    {
        return std::make_unique<execution::AbortTransactionOperator>(database.transaction_manager(), transaction,
                                                                     transaction_callback);
    }
    else if (typeid(*logical_node) == typeid(logical::CommitTransactionNode))
    {
        return std::make_unique<execution::CommitTransactionOperator>(database.transaction_manager(), transaction,
                                                                      transaction_callback);
    }

    return {};
}

Plan Builder::build_index_plan(Database &database, concurrency::Transaction *transaction, const std::string &table_name,
                               const std::string &column_name, const std::string &index_name)
{
    auto table = database.table(table_name);
    auto schema = table->schema();
    auto scan_operator = std::make_unique<execution::SequentialScanOperator>(
        transaction, static_cast<std::uint32_t>(database.config()[Config::k_ScanPageLimit]), std::move(schema),
        database.buffer_manager(), database.table_disk_manager(), *table);
    const auto column_index = scan_operator->schema().column_index(column_name);
    assert(column_index.has_value());

    auto build_index_operator = std::make_unique<execution::BuildIndexOperator>(database, transaction, table_name,
                                                                                column_index.value(), index_name);

    build_index_operator->data_operator(std::move(scan_operator));

    return Plan(database, std::move(build_index_operator));
}

std::unique_ptr<beedb::execution::PredicateMatcherInterface> Builder::build_predicate(
    const std::unique_ptr<expression::Operation> &predicate, const table::Schema &schema)
{
    if (predicate->is_binary())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

        if (predicate->type() == expression::Operation::Type::And)
        {
            auto left = Builder::build_predicate(binary->left_child(), schema);
            auto right = Builder::build_predicate(binary->right_child(), schema);
            return std::make_unique<execution::AndMatcher>(std::move(left), std::move(right));
        }
        else if (predicate->type() == expression::Operation::Type::Or)
        {
            auto left = Builder::build_predicate(binary->left_child(), schema);
            auto right = Builder::build_predicate(binary->right_child(), schema);
            return std::make_unique<execution::OrMatcher>(std::move(left), std::move(right));
        }
        else if (predicate->is_comparison())
        {
            auto &left = binary->left_child();
            auto &right = binary->right_child();
            if (left->is_nullary() && right->is_nullary())
            {
                auto *left_nullary = reinterpret_cast<expression::NullaryOperation *>(left.get());
                auto *right_nullary = reinterpret_cast<expression::NullaryOperation *>(right.get());
                if (left_nullary->term().is_attribute() && right_nullary->term().is_attribute())
                {
                    const auto optional_left_index = schema.column_index(left_nullary->term());
                    const auto optional_right_index = schema.column_index(right_nullary->term());
                    if (optional_left_index.has_value() == false || optional_right_index.has_value() == false)
                    {
                        throw exception::ExecutionException(
                            "Failed to interpret predicate: Can not reference attributes");
                    }

                    const auto left_index = optional_left_index.value();
                    const auto right_index = optional_right_index.value();
                    if (predicate->type() == expression::Operation::Type::Equals)
                    {
                        return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::EQ>>(
                            left_index, right_index);
                    }
                    else if (predicate->type() == expression::Operation::Type::NotEquals)
                    {
                        return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::NEQ>>(
                            left_index, right_index);
                    }
                    else if (predicate->type() == expression::Operation::Type::LesserEquals)
                    {
                        return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::LE>>(
                            left_index, right_index);
                    }
                    else if (predicate->type() == expression::Operation::Type::Lesser)
                    {
                        return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::LT>>(
                            left_index, right_index);
                    }
                    else if (predicate->type() == expression::Operation::Type::GreaterEquals)
                    {
                        return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::GE>>(
                            left_index, right_index);
                    }
                    else if (predicate->type() == expression::Operation::Type::Greater)
                    {
                        return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::GT>>(
                            left_index, right_index);
                    }
                }
                else if (left_nullary->term().is_attribute() && right_nullary->term().is_value())
                {
                    const auto optional_left_index = schema.column_index(left_nullary->term());
                    if (optional_left_index.has_value() == false)
                    {
                        throw exception::ExecutionException(
                            "Failed to interpret predicate: Can not reference attributes");
                    }

                    const auto left_index = optional_left_index.value();
                    const table::Type::Id column_type = schema.column(left_index).type();
                    const auto &term = reinterpret_cast<expression::NullaryOperation *>(right.get())->term();

                    if (predicate->type() == expression::Operation::Type::Equals)
                    {
                        return std::make_unique<
                            execution::AttributeValueMatcher<execution::PredicateMatcherInterface::EQ>>(
                            left_index, Builder::build_value(term, column_type));
                    }
                    else if (predicate->type() == expression::Operation::Type::NotEquals)
                    {
                        return std::make_unique<
                            execution::AttributeValueMatcher<execution::PredicateMatcherInterface::NEQ>>(
                            left_index, Builder::build_value(term, column_type));
                    }
                    else if (predicate->type() == expression::Operation::Type::LesserEquals)
                    {
                        return std::make_unique<
                            execution::AttributeValueMatcher<execution::PredicateMatcherInterface::LE>>(
                            left_index, Builder::build_value(term, column_type));
                    }
                    else if (predicate->type() == expression::Operation::Type::Lesser)
                    {
                        return std::make_unique<
                            execution::AttributeValueMatcher<execution::PredicateMatcherInterface::LT>>(
                            left_index, Builder::build_value(term, column_type));
                    }
                    else if (predicate->type() == expression::Operation::Type::GreaterEquals)
                    {
                        return std::make_unique<
                            execution::AttributeValueMatcher<execution::PredicateMatcherInterface::GE>>(
                            left_index, Builder::build_value(term, column_type));
                    }
                    else if (predicate->type() == expression::Operation::Type::Greater)
                    {
                        return std::make_unique<
                            execution::AttributeValueMatcher<execution::PredicateMatcherInterface::GT>>(
                            left_index, Builder::build_value(term, column_type));
                    }
                }
            }
        }
    }

    return nullptr;
}

std::unique_ptr<beedb::execution::PredicateMatcherInterface> Builder::build_predicate(
    const std::unique_ptr<expression::Operation> &predicate, const table::Schema &left_schema,
    const table::Schema &right_schema)
{
    if (predicate->type() == expression::Operation::Type::And)
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return std::make_unique<execution::AndMatcher>(
            Builder::build_predicate(binary->left_child(), left_schema, right_schema),
            Builder::build_predicate(binary->right_child(), left_schema, right_schema));
    }
    else if (predicate->type() == expression::Operation::Type::Or)
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return std::make_unique<execution::OrMatcher>(
            Builder::build_predicate(binary->left_child(), left_schema, right_schema),
            Builder::build_predicate(binary->right_child(), left_schema, right_schema));
    }
    else if (predicate->is_comparison())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        const auto &left = binary->left_child();
        const auto &right = binary->right_child();
        assert(left->is_nullary() && right->is_nullary());

        const auto &left_term = reinterpret_cast<expression::NullaryOperation *>(left.get())->term();
        const auto &right_term = reinterpret_cast<expression::NullaryOperation *>(right.get())->term();

        std::optional<table::Schema::ColumnIndexType> left_index = std::nullopt;
        if (left_term.is_attribute())
        {
            if (left_schema.contains(left_term))
            {
                left_index = left_schema.column_index(left_term);
            }
            else if (left_schema.contains(right_term))
            {
                left_index = left_schema.column_index(right_term);
            }
        }

        std::optional<table::Schema::ColumnIndexType> right_index = std::nullopt;
        if (right_term.is_attribute())
        {
            if (right_schema.contains(right_term))
            {
                right_index = right_schema.column_index(right_term);
            }
            else if (right_schema.contains(left_term))
            {
                right_index = right_schema.column_index(left_term);
            }
        }

        if (predicate->type() == expression::Operation::Type::Equals)
        {
            if (left_index.has_value() && right_index.has_value())
            {
                return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::EQ>>(
                    left_index.value(), right_index.value());
            }
            else if (left_index.has_value())
            {
                const auto &type = left_schema.column(left_index.value()).type();
                return std::make_unique<execution::AttributeValueMatcher<execution::PredicateMatcherInterface::EQ>>(
                    left_index.value(), Builder::build_value(right_term, type));
            }
        }
        else if (predicate->type() == expression::Operation::Type::NotEquals)
        {
            if (left_index.has_value() && right_index.has_value())
            {
                return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::NEQ>>(
                    left_index.value(), right_index.value());
            }
            else if (left_index.has_value())
            {
                const auto &type = left_schema.column(left_index.value()).type();
                return std::make_unique<execution::AttributeValueMatcher<execution::PredicateMatcherInterface::NEQ>>(
                    left_index.value(), Builder::build_value(right_term, type));
            }
        }
        else if (predicate->type() == expression::Operation::Type::LesserEquals)
        {
            if (left_index.has_value() && right_index.has_value())
            {
                return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::LE>>(
                    left_index.value(), right_index.value());
            }
            else if (left_index.has_value())
            {
                const auto &type = left_schema.column(left_index.value()).type();
                return std::make_unique<execution::AttributeValueMatcher<execution::PredicateMatcherInterface::LE>>(
                    left_index.value(), Builder::build_value(right_term, type));
            }
        }
        else if (predicate->type() == expression::Operation::Type::Lesser)
        {
            if (left_index.has_value() && right_index.has_value())
            {
                return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::LT>>(
                    left_index.value(), right_index.value());
            }
            else if (left_index.has_value())
            {
                const auto &type = left_schema.column(left_index.value()).type();
                return std::make_unique<execution::AttributeValueMatcher<execution::PredicateMatcherInterface::LT>>(
                    left_index.value(), Builder::build_value(right_term, type));
            }
        }
        else if (predicate->type() == expression::Operation::Type::GreaterEquals)
        {
            if (left_index.has_value() && right_index.has_value())
            {
                return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::GE>>(
                    left_index.value(), right_index.value());
            }
            else if (left_index.has_value())
            {
                const auto &type = left_schema.column(left_index.value()).type();
                return std::make_unique<execution::AttributeValueMatcher<execution::PredicateMatcherInterface::GE>>(
                    left_index.value(), Builder::build_value(right_term, type));
            }
        }
        else if (predicate->type() == expression::Operation::Type::Greater)
        {
            if (left_index.has_value() && right_index.has_value())
            {
                return std::make_unique<execution::AttributeMatcher<execution::PredicateMatcherInterface::GT>>(
                    left_index.value(), right_index.value());
            }
            else if (left_index.has_value())
            {
                const auto &type = left_schema.column(left_index.value()).type();
                return std::make_unique<execution::AttributeValueMatcher<execution::PredicateMatcherInterface::GT>>(
                    left_index.value(), Builder::build_value(right_term, type));
            }
        }
    }

    return nullptr;
}

beedb::table::Value Builder::build_value(const expression::Term &term, table::Type::Id type)
{
    table::Value::value_type value;
    std::visit(
        [&value, &type](const auto &op) {
            using T = std::decay_t<decltype(op)>;
            if constexpr (std::is_same<std::string, T>::value)
            {
                if (type == table::Type::CHAR)
                {
                    value = op;
                }
                else if (type == table::Type::DATE)
                {
                    value = table::Date::from_string(op);
                }
            }
            else if constexpr (std::is_same<std::int64_t, T>::value)
            {
                if (type == table::Type::INT)
                {
                    value = static_cast<std::int32_t>(op);
                }
                else if (type == table::Type::LONG)
                {
                    value = static_cast<std::int64_t>(op);
                }
                else if (type == table::Type::DECIMAL)
                {
                    value = static_cast<double>(op);
                }
            }
            else if constexpr (std::is_same<double, T>::value)
            {
                if (type == table::Type::DECIMAL)
                {
                    value = static_cast<double>(op);
                }
            }
        },
        term.attribute_or_value());

    return table::Value{type, std::move(value)};
}

beedb::table::Value Builder::build_value(table::Value &&value_in, table::Type::Id type)
{
    table::Value::value_type value_out;
    std::visit(
        [&value_out, &type](auto &op) {
            using T = std::decay_t<decltype(op)>;
            if constexpr (std::is_same<std::string, T>::value)
            {
                if (type == table::Type::CHAR)
                {
                    value_out = std::move(op);
                }
                else if (type == table::Type::DATE)
                {
                    value_out = table::Date::from_string(op);
                }
            }
            else if constexpr (std::is_same<std::int64_t, T>::value)
            {
                if (type == table::Type::INT)
                {
                    value_out = static_cast<std::int32_t>(op);
                }
                else if (type == table::Type::LONG)
                {
                    value_out = static_cast<std::int64_t>(op);
                }
                else if (type == table::Type::DECIMAL)
                {
                    value_out = static_cast<double>(op);
                }
            }
            else if constexpr (std::is_same<double, T>::value)
            {
                if (type == table::Type::DECIMAL)
                {
                    value_out = static_cast<double>(op);
                }
            }
        },
        value_in.value());

    return table::Value{type, std::move(value_out)};
}

beedb::table::Value Builder::build_value(const expression::Term &term)
{
    const auto &value = term.attribute_or_value();
    if (std::holds_alternative<std::int64_t>(value))
    {
        return Builder::build_value(term, table::Type::Id::LONG);
    }
    else if (std::holds_alternative<std::int32_t>(value))
    {
        return Builder::build_value(term, table::Type::Id::INT);
    }
    else if (std::holds_alternative<double>(value))
    {
        return Builder::build_value(term, table::Type::Id::DECIMAL);
    }
    else
    {
        throw exception::ExecutionException("Can not infer type of value.");
    }
}

beedb::table::Tuple Builder::build_tuple(const table::Schema &schema,
                                         const std::vector<table::Schema::ColumnIndexType> &column_indices,
                                         const std::vector<table::Schema::ColumnIndexType> &default_column_indices,
                                         std::vector<table::Value> &values)
{
    table::Tuple tuple(schema, schema.row_size());
    for (auto i = 0u; i < values.size(); ++i)
    {
        const auto index = column_indices[i];
        tuple.set(index, Builder::build_value(std::move(values[i]), schema.column(index).type()));
    }

    for (const auto index : default_column_indices)
    {
        tuple.set(index, table::Value::make_null(schema.column(index).type()));
    }

    return tuple;
}

std::unordered_set<beedb::execution::KeyRange> Builder::extract_key_ranges(
    const std::unique_ptr<beedb::expression::Operation> &predicate)
{
    if (predicate->is_logical_connective())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        auto left = Builder::extract_key_ranges(binary->left_child());
        auto right = Builder::extract_key_ranges(binary->right_child());
        left.merge(std::move(right));
        return left;
    }
    else if (predicate->is_comparison())
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (binary->left_child()->is_nullary() && binary->right_child()->is_nullary())
        {
            auto key_range = std::unordered_set<execution::KeyRange>{};
            auto *left = reinterpret_cast<expression::NullaryOperation *>(binary->left_child().get());
            auto *right = reinterpret_cast<expression::NullaryOperation *>(binary->right_child().get());
            if (left->term().is_value() && right->term().is_attribute()) // e.g. 5 < id
            {
                const auto &key = Builder::extract_key(left->term());
                if (key.has_value() == false)
                {
                    return key_range;
                }

                if (binary->type() == expression::Operation::Type::Equals)
                {
                    key_range.insert(execution::KeyRange{key.value()});
                }
                else if (binary->type() == expression::Operation::Type::Lesser)
                {
                    key_range.insert(execution::KeyRange{key.value() + 1, std::numeric_limits<std::int64_t>::max()});
                }
                else if (binary->type() == expression::Operation::Type::LesserEquals)
                {
                    key_range.insert(execution::KeyRange{key.value(), std::numeric_limits<std::int64_t>::max()});
                }
                else if (binary->type() == expression::Operation::Type::Greater)
                {
                    key_range.insert(
                        execution::KeyRange{std::numeric_limits<std::int64_t>::min() + 1, key.value() - 1});
                }
                else if (binary->type() == expression::Operation::Type::GreaterEquals)
                {
                    key_range.insert(execution::KeyRange{std::numeric_limits<std::int64_t>::min() + 1, key.value()});
                }
            }
            else if (left->term().is_attribute() && right->term().is_value()) // eg. id < 5
            {
                const auto &key = Builder::extract_key(right->term());
                if (key.has_value() == false)
                {
                    return key_range;
                }

                if (binary->type() == expression::Operation::Type::Equals)
                {
                    key_range.insert(execution::KeyRange{key.value()});
                }
                else if (binary->type() == expression::Operation::Type::Lesser)
                {
                    key_range.insert(
                        execution::KeyRange{std::numeric_limits<std::int64_t>::min() + 1, key.value() - 1});
                }
                else if (binary->type() == expression::Operation::Type::LesserEquals)
                {
                    key_range.insert(execution::KeyRange{std::numeric_limits<std::int64_t>::min() + 1, key.value()});
                }
                else if (binary->type() == expression::Operation::Type::Greater)
                {
                    key_range.insert(execution::KeyRange{key.value() + 1, std::numeric_limits<std::int64_t>::max()});
                }
                else if (binary->type() == expression::Operation::Type::GreaterEquals)
                {
                    key_range.insert(execution::KeyRange{key.value(), std::numeric_limits<std::int64_t>::max()});
                }
            }

            return key_range;
        }
    }

    return std::unordered_set<execution::KeyRange>{};
}

std::optional<std::int64_t> Builder::extract_key(const expression::Term &term)
{
    const auto &value = term.attribute_or_value();
    if (std::holds_alternative<std::int64_t>(value))
    {
        return std::make_optional(std::get<std::int64_t>(value));
    }
    else if (std::holds_alternative<std::int32_t>(value))
    {
        return std::make_optional(std::int64_t(std::get<std::int64_t>(value)));
    }

    return std::nullopt;
}

std::pair<beedb::table::Type::Id, std::unique_ptr<beedb::execution::ArithmeticCalculatorInterface>> Builder::
    build_arithmetic_calculator(const std::unique_ptr<expression::Operation> &expression,
                                const table::Schema &child_schema)
{
    auto *binary = reinterpret_cast<expression::BinaryOperation *>(expression.get());
    const auto is_left_attribute = binary->left_child()->result()->is_attribute();
    const auto is_right_attribute = binary->right_child()->result()->is_attribute();

    if (is_left_attribute && is_right_attribute)
    {
        const auto left_index = child_schema.column_index(binary->left_child()->result().value());
        if (left_index.has_value())
        {
            const auto right_index = child_schema.column_index(binary->right_child()->result().value());
            if (right_index.has_value())
            {
                std::unique_ptr<execution::ArithmeticCalculatorInterface> calculator{nullptr};
                if (binary->type() == expression::Operation::Type::Add)
                {
                    calculator = std::make_unique<
                        execution::AttributeArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Add>>(
                        left_index.value(), right_index.value());
                }
                else if (binary->type() == expression::Operation::Type::Sub)
                {
                    calculator = std::make_unique<
                        execution::AttributeArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Sub>>(
                        left_index.value(), right_index.value());
                }
                else if (binary->type() == expression::Operation::Type::Multiply)
                {
                    calculator = std::make_unique<execution::AttributeArithmeticCalculator<
                        execution::ArithmeticCalculatorInterface::Type::Multiply>>(left_index.value(),
                                                                                   right_index.value());
                }
                else if (binary->type() == expression::Operation::Type::Divide)
                {
                    calculator = std::make_unique<execution::AttributeArithmeticCalculator<
                        execution::ArithmeticCalculatorInterface::Type::Divide>>(left_index.value(),
                                                                                 right_index.value());
                }

                return std::make_pair(child_schema.column(left_index.value()).type(), std::move(calculator));
            }
        }
    }
    else if (is_left_attribute && is_right_attribute == false)
    {
        const auto left_index = child_schema.column_index(binary->left_child()->result().value());
        if (left_index.has_value())
        {
            auto value = Builder::build_value(binary->right_child()->result().value(),
                                              child_schema.column(left_index.value()).type());

            std::unique_ptr<execution::ArithmeticCalculatorInterface> calculator{nullptr};
            if (binary->type() == expression::Operation::Type::Add)
            {
                calculator = std::make_unique<
                    execution::AttributeValueArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Add>>(
                    left_index.value(), std::move(value));
            }
            else if (binary->type() == expression::Operation::Type::Sub)
            {
                calculator = std::make_unique<
                    execution::AttributeValueArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Sub>>(
                    left_index.value(), std::move(value));
            }
            else if (binary->type() == expression::Operation::Type::Multiply)
            {
                calculator = std::make_unique<execution::AttributeValueArithmeticCalculator<
                    execution::ArithmeticCalculatorInterface::Type::Multiply>>(left_index.value(), std::move(value));
            }
            else if (binary->type() == expression::Operation::Type::Divide)
            {
                calculator = std::make_unique<execution::AttributeValueArithmeticCalculator<
                    execution::ArithmeticCalculatorInterface::Type::Divide>>(left_index.value(), std::move(value));
            }

            return std::make_pair(child_schema.column(left_index.value()).type(), std::move(calculator));
        }
    }
    else if (is_left_attribute == false && is_right_attribute)
    {
        const auto right_index = child_schema.column_index(binary->right_child()->result().value());
        if (right_index.has_value())
        {
            auto value = Builder::build_value(binary->left_child()->result().value(),
                                              child_schema.column(right_index.value()).type());

            std::unique_ptr<execution::ArithmeticCalculatorInterface> calculator{nullptr};
            if (binary->type() == expression::Operation::Type::Add)
            {
                calculator = std::make_unique<
                    execution::ValueAttributeArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Add>>(
                    std::move(value), right_index.value());
            }
            else if (binary->type() == expression::Operation::Type::Sub)
            {
                calculator = std::make_unique<
                    execution::ValueAttributeArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Sub>>(
                    std::move(value), right_index.value());
            }
            else if (binary->type() == expression::Operation::Type::Multiply)
            {
                calculator = std::make_unique<execution::ValueAttributeArithmeticCalculator<
                    execution::ArithmeticCalculatorInterface::Type::Multiply>>(std::move(value), right_index.value());
            }
            else if (binary->type() == expression::Operation::Type::Divide)
            {
                calculator = std::make_unique<execution::ValueAttributeArithmeticCalculator<
                    execution::ArithmeticCalculatorInterface::Type::Divide>>(std::move(value), right_index.value());
            }

            return std::make_pair(child_schema.column(right_index.value()).type(), std::move(calculator));
        }
    }
    else if (is_left_attribute == false && is_right_attribute == false)
    {
        auto left_value = Builder::build_value(binary->left_child()->result().value());
        auto right_value = Builder::build_value(binary->right_child()->result().value());
        const auto type = left_value.type();

        std::unique_ptr<execution::ArithmeticCalculatorInterface> calculator{nullptr};
        if (binary->type() == expression::Operation::Type::Add)
        {
            calculator = std::make_unique<
                execution::ValueArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Add>>(
                std::move(left_value), std::move(right_value));
        }
        else if (binary->type() == expression::Operation::Type::Sub)
        {
            calculator = std::make_unique<
                execution::ValueArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Sub>>(
                std::move(left_value), std::move(right_value));
        }
        else if (binary->type() == expression::Operation::Type::Multiply)
        {
            calculator = std::make_unique<
                execution::ValueArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Multiply>>(
                std::move(left_value), std::move(right_value));
        }
        else if (binary->type() == expression::Operation::Type::Divide)
        {
            calculator = std::make_unique<
                execution::ValueArithmeticCalculator<execution::ArithmeticCalculatorInterface::Type::Divide>>(
                std::move(left_value), std::move(right_value));
        }

        return std::make_pair(type, std::move(calculator));
    }

    return std::make_pair(table::Type::Id::INT, nullptr);
}