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
#include "plan.h"
#include <concurrency/transaction.h>
#include <concurrency/transaction_callback.h>
#include <database.h>
#include <execution/arithmetic_calculator.h>
#include <execution/index_scan_operator.h>
#include <execution/operator_interface.h>
#include <execution/selection_operator.h>
#include <memory>
#include <plan/logical/node/node_interface.h>
#include <unordered_set>

namespace beedb::plan::physical
{
/**
 * The Builder builds the physical operator plan based on the logical plan.
 */
class Builder
{
  public:
    /**
     * Creates a plan containing physical operators
     * based on the logical plan.
     *
     * @param database Database for the execution.
     * @param transaction Transaction for the execution.
     * @param logical_plan Logical plan as base for physical operators.
     * @return Plan with physical operators.
     */
    static Plan build(Database &database, concurrency::Transaction *transaction,
                      concurrency::TransactionCallback &transaction_callback, bool add_to_scan_set,
                      const std::unique_ptr<logical::NodeInterface> &logical_plan);

    /**
     * Creates a plan for filling index data structures with data.
     *
     * @param database Database for execution.
     * @param transaction Transaction for the execution.
     * @param table_name Name of the indexed table.
     * @param column_name Name of the indexed column.
     * @param index_name Name of the index.
     * @return Plan for filling the index.
     */
    static Plan build_index_plan(Database &database, concurrency::Transaction *transaction,
                                 const std::string &table_name, const std::string &column_name,
                                 const std::string &index_name);

  private:
    /**
     * Builds a physical execution operator based on a logical node.
     *
     * @param database Database for execution.
     * @param transaction Transaction for execution.
     * @param logical_plan Full logical plan.
     * @param logical_node_name Name of the logical node.
     * @return Pointer to the built physical operator.
     */
    static std::unique_ptr<execution::OperatorInterface> build_operator(
        Database &database, concurrency::Transaction *transaction,
        concurrency::TransactionCallback &transaction_callback, bool add_to_scan_set,
        concurrency::ScanSetItem *scan_set, const std::unique_ptr<logical::NodeInterface> &logical_node);

    /**
     * Turns a logical predicate into a physical predicate matcher.
     *
     * @param predicate Logical predicate.
     * @param schema Schema for the table, the predicate will be evaluated on.
     * @return Pointer to the predicate matcher.
     */
    static std::unique_ptr<execution::PredicateMatcherInterface> build_predicate(
        const std::unique_ptr<expression::Operation> &, const table::Schema &schema);

    /**
     * Turns a logical join predicate into a physical predicate matcher.
     *
     * @param predicate Logical predicate.
     * @param left_schema Schema of the left join table.
     * @param right_schema Schema of the right join table.
     * @return Pointer to the predicate matcher.
     */
    static std::unique_ptr<execution::PredicateMatcherInterface> build_predicate(
        const std::unique_ptr<expression::Operation> &predicate, const table::Schema &left_schema,
        const table::Schema &right_schema);

    /**
     * Builds a physical value from a logical operand for the given type.
     *
     * @param operand Logical operand.
     * @param type Type for the new value.
     * @return Physical value.
     */
    static table::Value build_value(const expression::Term &term, table::Type::Id type);

    /**
     * Builds a physical value from a logical operand for the given type.
     *
     * @param operand Logical operand.
     * @param type Type for the new value.
     * @return Physical value.
     */
    static table::Value build_value(table::Value &&value, table::Type::Id type);

    static table::Value build_value(const expression::Term &term);

    /**
     * Builds a physical tuple based on the required schema and logical operands.
     *
     * @param schema Schema of the target table.
     * @param attributes Logical schema.
     * @param values Logical operands that represent the values.
     * @return Physical tuple.
     */
    static table::Tuple build_tuple(const table::Schema &schema,
                                    const std::vector<table::Schema::ColumnIndexType> &column_indices,
                                    const std::vector<table::Schema::ColumnIndexType> &default_column_indices,
                                    std::vector<table::Value> &values);

    /**
     * Extracts key ranges for index scans from a logical predicate.
     *
     * @param predicate Logical predicate.
     * @return Set of key ranges the index scan has to lookup.
     */
    static std::unordered_set<execution::KeyRange> extract_key_ranges(
        const std::unique_ptr<expression::Operation> &predicate);

    /**
     * Extracts a key from a logical atom.
     * @param atom Logical atom.
     * @return Key from the atom or std::nullopt, when no key was represented by the atom.
     */
    static std::optional<std::int64_t> extract_key(const expression::Term &term);

    static std::pair<table::Type::Id, std::unique_ptr<execution::ArithmeticCalculatorInterface>>
    build_arithmetic_calculator(const std::unique_ptr<expression::Operation> &expression,
                                const table::Schema &child_schema);
};
} // namespace beedb::plan::physical
