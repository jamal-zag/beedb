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
#include <memory>
#include <plan/logical/node/node_interface.h>
#include <plan/optimizer/rule/optimizer_rule_interface.h>
#include <unordered_map>
#include <vector>

namespace beedb::plan::logical
{
class Optimizer
{
  public:
    Optimizer() = default;
    virtual ~Optimizer() = default;

    std::unique_ptr<NodeInterface> optimize(std::unique_ptr<NodeInterface> &&logical_plan);

    std::unique_ptr<NodeInterface> operator()(std::unique_ptr<NodeInterface> &&logical_plan)
    {
        return optimize(std::move(logical_plan));
    }

    void add(std::unique_ptr<OptimizerRuleInterface> &&rule)
    {
        _rules.emplace_back(std::move(rule));
    }

  private:
    std::vector<std::unique_ptr<OptimizerRuleInterface>> _rules;

    static std::unique_ptr<NodeInterface> commit(PlanView &&plan_view, std::unique_ptr<NodeInterface> &&plan);
    static std::unique_ptr<NodeInterface> commit(
        PlanView::node_t node, const PlanView &plan_view,
        std::unordered_map<std::uintptr_t, std::unique_ptr<NodeInterface>> &original_nodes);
    static void steal_nodes(std::unique_ptr<NodeInterface> &&node,
                            std::unordered_map<std::uintptr_t, std::unique_ptr<NodeInterface>> &node_container);
};

class RequiredOptimizer final : public Optimizer
{
  public:
    explicit RequiredOptimizer(Database &database);
    ~RequiredOptimizer() override = default;
};

class CompleteOptimizer final : public Optimizer
{
  public:
    explicit CompleteOptimizer(Database &database);
    ~CompleteOptimizer() override = default;
};
} // namespace beedb::plan::logical