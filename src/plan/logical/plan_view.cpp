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

#include <plan/logical/plan_view.h>
#include <unordered_set>

using namespace beedb::plan::logical;

void PlanView::replace(PlanView::node_t original_node, PlanView::node_t new_node)
{
    if (this->_node_parent.find(original_node) != this->_node_parent.end())
    {
        auto parent = this->_node_parent.at(original_node);
        this->_node_parent.erase(original_node);
        this->_node_parent.insert(std::make_pair(new_node, parent));

        if (this->_node_children.find(parent) != this->_node_children.end())
        {
            auto &children = this->_node_children.at(parent);
            if (children[0] == original_node)
            {
                children[0] = new_node;
            }
            else if (children[1] == original_node)
            {
                children[1] = new_node;
            }
        }
    }

    std::vector<std::pair<node_t, node_t>> new_parents;
    for (auto &child : this->_node_parent)
    {
        if (std::get<1>(child) == original_node)
        {
            new_parents.emplace_back(std::make_pair(std::get<0>(child), new_node));
        }
    }

    //    this->_node_parent.erase(std::remove_if(this->_node_parent.begin(), this->_node_parent.end(),
    //    [original_node](const auto& child) {
    //        return std::get<1>(child) == original_node;
    //    }), this->_node_parent.end());
    for (auto it = this->_node_parent.begin(); it != this->_node_parent.end();)
    {
        if (std::get<1>(*it) == original_node)
        {
            it = this->_node_parent.erase(it);
        }
        else
        {
            ++it;
        }
    }
    for (auto &parent : new_parents)
    {
        this->_node_parent.insert(std::move(parent));
    }

    if (this->_node_children.find(original_node) != this->_node_children.end())
    {
        auto &children = this->_node_children.at(original_node);
        this->_node_children.insert(std::make_pair(new_node, children));
        this->_node_children.erase(original_node);
    }
}

bool PlanView::move_between(node_t node, node_t child_node, node_t node_to_move)
{
    if (node_to_move->is_unary())
    {
        this->erase(node_to_move);
        this->insert_between(node, child_node, node_to_move);
        return true;
    }

    return false;
}

PlanView::node_t PlanView::root() const
{
    for (auto [node, parent] : this->_node_parent)
    {
        if (parent == nullptr)
        {
            return node;
        }
    }

    return nullptr;
}

void PlanView::insert_between(node_t first, node_t second, node_t node_to_insert)
{
    if (node_to_insert->is_unary())
    {
        // Was:     first -> second
        // Will be: first -> node_to_insert -> second

        // Set node_to_insert as child of first
        auto &children = this->_node_children[first];
        std::replace(children.begin(), children.end(), second, node_to_insert);

        // Set first as parent of node_to_insert
        this->_node_parent[node_to_insert] = first;

        // Set second as child of node_to_insert
        this->_node_children[node_to_insert] = {second, nullptr};
        this->_node_parent[second] = node_to_insert;
    }
}

void PlanView::erase(node_t node)
{
    auto *node_to_move_parent = this->_node_parent[node];
    auto node_to_move_children = this->_node_children[node];

    // Children have new parent (the parent of node)
    if (node_to_move_children[0] != nullptr)
    {
        this->_node_parent[node_to_move_children[0]] = node_to_move_parent;
    }
    if (node_to_move_children[1] != nullptr)
    {
        this->_node_parent[node_to_move_children[1]] = node_to_move_parent;
    }

    // The parent of node has new children.
    if (node_to_move_parent != nullptr)
    {
        this->_node_children[node_to_move_parent] = node_to_move_children;
    }
    this->_node_children.erase(node);
    this->_node_parent.erase(node);
}

void PlanView::extract_nodes(PlanView::node_t parent, const std::unique_ptr<NodeInterface> &node)
{
    this->_node_parent.insert(std::make_pair(node.get(), parent));
    if (node->is_unary())
    {
        const auto &child = reinterpret_cast<UnaryNode *>(node.get())->child();
        auto children = std::array<node_t, 2>{nullptr};
        children[0] = child.get();
        this->_node_children.insert(std::make_pair(node.get(), children));
        extract_nodes(node.get(), child);
    }
    else if (node->is_binary())
    {
        const auto &left_child = reinterpret_cast<BinaryNode *>(node.get())->left_child();
        const auto &right_child = reinterpret_cast<BinaryNode *>(node.get())->right_child();
        std::array<node_t, 2> children = {left_child.get(), right_child.get()};
        this->_node_children.insert(std::make_pair(node.get(), children));
        extract_nodes(node.get(), left_child);
        extract_nodes(node.get(), right_child);
    }
}
