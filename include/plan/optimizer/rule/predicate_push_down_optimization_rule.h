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
#include <expression/operation.h>
#include <expression/term.h>
#include <memory>
#include <vector>

namespace beedb::plan::logical
{
class PredicatePushDownOptimizationRule final : public OptimizerRuleInterface
{
  public:
    PredicatePushDownOptimizationRule() = default;
    ~PredicatePushDownOptimizationRule() override = default;

    bool optimize(PlanView &plan) override;

  private:
    /**
     * Checks if the schema of a given node contains all
     * attributes needed by the given predicate.
     *
     * @param node Node to check.
     * @param predicate Predicate with attributes that are required.
     * @return True, if the schema of the node contains all needed attributes. False otherwise.
     */
    [[nodiscard]] static bool provides_needed_attributes(PlanView::node_t node,
                                                         const std::unique_ptr<expression::Operation> &predicate);

    /**
     * Determines the lowest node in the plan that could
     * work as the child of a given selection. To do so,
     * the node that is returned by this function has to
     * provide all attributes needed by the predicate.
     * The further down the logical plan, the better.
     *
     * @param plan  Logical plan.
     * @param current_node Node to start the search.
     * @param predicate Predicate to push down.
     * @return The node that could work as "the lowest" child.
     */
    [[nodiscard]] static PlanView::node_t determine_lowest_position(
        const PlanView &plan, PlanView::node_t current_node, const std::unique_ptr<expression::Operation> &predicate);
};
} // namespace beedb::plan::logical