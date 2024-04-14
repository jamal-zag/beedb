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
#include <cstdint>
#include <functional>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace beedb::plan::logical
{
class PlanView
{
  public:
    using node_t = NodeInterface *;
    using edge_t = std::tuple<node_t, node_t, bool>;

    PlanView(const std::unique_ptr<NodeInterface> &root)
    {
        extract_nodes(nullptr, root);
    }

    ~PlanView() = default;

    void replace(node_t original_node, node_t new_node);
    [[nodiscard]] bool move_between(node_t node, node_t child_node, node_t node_to_move);
    void erase(node_t node);

    [[nodiscard]] bool has_child(node_t node) const
    {
        return _node_children.find(node) != _node_children.end() && _node_children.at(node)[0] != nullptr;
    }
    [[nodiscard]] const std::array<node_t, 2> &children(node_t node) const
    {
        return _node_children.at(node);
    }
    [[nodiscard]] const std::unordered_map<node_t, std::array<node_t, 2>> &nodes_and_children() const
    {
        return _node_children;
    }
    [[nodiscard]] const std::unordered_map<node_t, node_t> &nodes_and_parent() const
    {
        return _node_parent;
    }

    [[nodiscard]] node_t parent(const node_t node) const
    {
        if (_node_parent.find(node) == _node_parent.end())
        {
            return nullptr;
        }

        return _node_parent.at(node);
    }

    node_t root() const;

  private:
    std::unordered_map<node_t, std::array<node_t, 2>> _node_children;
    std::unordered_map<node_t, node_t> _node_parent;

    void extract_nodes(node_t parent, const std::unique_ptr<NodeInterface> &node);
    void insert_between(node_t first, node_t second, node_t node_to_insert);
};
} // namespace beedb::plan::logical