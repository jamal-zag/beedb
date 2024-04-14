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

#include <iostream>
#include <plan/logical/node/scan_node.h>
#include <plan/logical/node/selection_node.h>
#include <plan/optimizer/rule/index_scan_optimization_rule.h>

using namespace beedb::plan::logical;

bool IndexScanOptimizationRule::optimize(PlanView &plan)
{
    for (auto [node, parent] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(TableScanNode) && typeid(*parent) == typeid(SelectionNode))
        {
            auto *table_scan_node = reinterpret_cast<TableScanNode *>(node);
            const auto &table_name = table_scan_node->table().table_name();
            const auto &selection_predicate = reinterpret_cast<SelectionNode *>(parent)->predicate();
            auto attribute_and_predicate = this->find_index_predicate(table_name, selection_predicate);
            if (std::get<0>(attribute_and_predicate).has_value())
            {
                auto *index_scan_node = new IndexScanNode(this->_database, TableReference{table_scan_node->table()},
                                                          std::move(std::get<0>(attribute_and_predicate).value()),
                                                          std::move(std::get<1>(attribute_and_predicate)));
                index_scan_node->schema(table_scan_node->schema());
                plan.replace(node, index_scan_node);
                return true;
            }
        }
    }

    return false;
}

std::pair<std::optional<beedb::expression::Attribute>, std::unique_ptr<beedb::expression::Operation>>
IndexScanOptimizationRule::find_index_predicate(const std::string &table_name,
                                                const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->is_logical_connective() && predicate->type() == expression::Operation::Type::And)
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        auto left_predicate = this->find_index_predicate(table_name, binary->left_child());
        auto right_predicate = this->find_index_predicate(table_name, binary->right_child());
        if (std::get<1>(left_predicate) != nullptr && std::get<1>(right_predicate) != nullptr)
        {
            return std::make_pair(
                std::move(std::get<0>(left_predicate)),
                std::make_unique<expression::BinaryOperation>(binary->type(), std::move(std::get<1>(left_predicate)),
                                                              std::move(std::get<1>(right_predicate))));
        }
        else if (std::get<1>(left_predicate) != nullptr)
        {
            return left_predicate;
        }
        else if (std::get<1>(right_predicate) != nullptr)
        {
            return right_predicate;
        }
    }
    else if (predicate->is_comparison() && predicate->type() != expression::Operation::Type::NotEquals)
    {
        auto *binary = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (binary->left_child()->is_nullary() && binary->right_child()->is_nullary())
        {
            auto *left = reinterpret_cast<expression::NullaryOperation *>(binary->left_child().get());
            auto *right = reinterpret_cast<expression::NullaryOperation *>(binary->right_child().get());
            if (left->term().is_attribute() && right->term().is_value())
            {
                auto *table = this->_database.table(table_name);
                const auto &attribute = left->term().get<expression::Attribute>();
                const auto column_index = table->schema().column_index(attribute.column_name());
                if (column_index.has_value())
                {
                    const auto &column = table->schema().column(column_index.value());
                    const auto is_range = binary->type() == expression::Operation::Type::LesserEquals ||
                                          binary->type() == expression::Operation::Type::Lesser ||
                                          binary->type() == expression::Operation::Type::Greater ||
                                          binary->type() == expression::Operation::Type::Greater;
                    if (column.is_indexed(is_range))
                    {
                        return std::make_pair(std::make_optional(attribute), binary->copy());
                    }
                }
            }
        }
    }

    return std::make_pair(std::nullopt, nullptr);
}