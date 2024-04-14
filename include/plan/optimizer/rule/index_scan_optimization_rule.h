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
#include "optimizer_rule_interface.h"
#include <database.h>
#include <expression/operation.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace beedb::plan::logical
{
class IndexScanOptimizationRule : public OptimizerRuleInterface
{
  public:
    explicit IndexScanOptimizationRule(Database &database) : _database(database)
    {
    }
    ~IndexScanOptimizationRule() override = default;

    bool optimize(PlanView &plan) override;

  private:
    Database &_database;
    std::pair<std::optional<expression::Attribute>, std::unique_ptr<expression::Operation>> find_index_predicate(
        const std::string &table_name, const std::unique_ptr<expression::Operation> &predicate);
};
} // namespace beedb::plan::logical