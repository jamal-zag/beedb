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
#include "node/node_interface.h"
#include <database.h>
#include <parser/node.h>

namespace beedb::plan::logical
{
class Builder
{
  public:
    static std::unique_ptr<NodeInterface> build(Database &database,
                                                const std::unique_ptr<parser::NodeInterface> &query);
    static std::unique_ptr<NodeInterface> build(Database &database, parser::SelectQuery *select_query);
    static std::unique_ptr<NodeInterface> build(Database &database, parser::CreateTableStatement *create_statement);
    static std::unique_ptr<NodeInterface> build(Database &database,
                                                parser::CreateIndexStatement *create_index_statement);
    static std::unique_ptr<NodeInterface> build(Database &database, parser::InsertStatement *insert_statement);
    static std::unique_ptr<NodeInterface> build(Database &database, parser::UpdateStatement *update_statement);
    static std::unique_ptr<NodeInterface> build(Database &database, parser::DeleteStatement *delete_statement);
    static std::unique_ptr<NodeInterface> build(Database &database,
                                                parser::TransactionStatement *transaction_statement);

  private:
    [[nodiscard]] static std::unique_ptr<NodeInterface> create_from(
        Database &database, std::vector<parser::TableDescr> &&from,
        std::optional<std::vector<parser::JoinDescr>> &&join);
    [[nodiscard]] static std::unique_ptr<NodeInterface> parse_select_expression(
        std::unique_ptr<NodeInterface> &&top_node, std::vector<expression::Term> &projection_terms,
        std::vector<std::unique_ptr<expression::Operation>> &aggregations,
        std::unique_ptr<expression::Operation> &&select_expression, bool add_to_projection);
    [[nodiscard]] static std::vector<std::unique_ptr<expression::Operation>> split_logical_and(
        std::unique_ptr<expression::Operation> &&root);
    static void split_logical_and(std::unique_ptr<expression::Operation> &&root,
                                  std::vector<std::unique_ptr<expression::Operation>> &container);
};
} // namespace beedb::plan::logical
