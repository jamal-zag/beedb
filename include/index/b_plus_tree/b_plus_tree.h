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
#include "b_plus_tree_node.h"
#include <cstdint>
#include <index/non_unique_index_interface.h>
#include <index/return_value.h>
#include <optional>
#include <ostream>
#include <set>
#include <utility>
#include <vector>

namespace beedb::index::bplustree
{
template <typename K, typename V, bool U> class BPlusTree
{
  public:
    using Node = BPlusTreeNode<K, V, U>;

    BPlusTree() : _root(new Node(true))
    {
    }

    ~BPlusTree()
    {
        delete _root;
    }

    /**
     * Inserts the given key-value-pair into the tree.
     *
     * @param key
     * @param value
     */
    void put(const K key, V value);

    /**
     * Finds the value by the given key.
     *
     * @param key
     * @return The found value.
     */
    [[nodiscard]] std::optional<typename ReturnValue<V, U>::type> get(const K key) const;

    [[nodiscard]] std::optional<std::set<V>> get(const K key_from, const K key_to) const;

    [[nodiscard]] Node *root() const
    {
        return _root;
    }

    [[nodiscard]] size_type height() const
    {
        return _height;
    }

  private:
    Node *_root;
    size_type _height = 1;

    /**
     * Locates a leaf node for a given key.
     *
     * @param key
     * @param node_path
     * @return
     */
    Node *locate_leaf(const K key, std::vector<Node *> *node_path = nullptr) const;

    /**
     * Inserts the given key-value-tuple into the give leaf node.
     *
     * @param leaf_node
     * @param key
     * @param value
     * @return
     */
    Node *insert_into_leaf(Node *leaf_node, const K key, const V value);

    /**
     * Inserts the given key and separator into the given inner node.
     *
     * @param inner_node
     * @param key
     * @param separator
     * @return
     */
    std::pair<Node *, K> insert_into_inner(Node *inner_node, const K key, Node *separator);

    /**
     * Creates a new root with pointer to the two given new child nodes.
     *
     * @param left
     * @param right
     * @param key
     */
    void install_new_root_node(Node *left, Node *right, const K key);

    /**
     * Splits the given inner node and returns the new node and a key,
     * that has to be inserted into the parent node.
     *
     * @param inner_node
     * @param key
     * @param separator
     * @return
     */
    std::pair<Node *, K> split_inner_node(Node *inner_node, const K key, Node *separator);

    /**
     * Splits the given leaf node and returns the new node.
     *
     * @param leaf_node
     * @return
     */
    Node *split_leaf_node(Node *leaf_node);

    friend std::ostream &operator<<(std::ostream &stream, const BPlusTree<K, V, U> &tree)
    {
        Node *root = tree.root();
        if (root == nullptr)
        {
            return stream;
        }

        const auto items = tree.root()->size_include_children();
        const auto nodes = tree.root()->count_children();

        return stream << "Height          = " << tree.height() << "\n"
                      << "Key-Value-Pairs = " << items.second << "\n"
                      << "Inner-Nodes     = " << nodes.first << "\n"
                      << "Leaf-Nodes      = " << nodes.second << "\n"
                      << "Memory          = "
                      << ((nodes.first + nodes.second) * Config::b_plus_tree_page_size) / 1024 / 1024 << " MB\n";
    }
};

template <typename K, typename V, bool U> void BPlusTree<K, V, U>::put(const K key, V value)
{
    // Path for traversal. All nodes from root excluding the leaf node will be stored.
    std::vector<Node *> path;
    path.reserve(6);

    // Locate the possible leaf.
    Node *leaf = this->locate_leaf(key, &path);

    // Insert into leaf
    K up_key;
    Node *new_node = this->insert_into_leaf(leaf, key, value);
    if (new_node != nullptr)
    {
        up_key = new_node->leaf_key(0u);
    }

    // Propagate up.
    while (new_node != nullptr && path.empty() == false)
    {
        Node *parent = path.back();
        path.pop_back();
        auto [n, u] = this->insert_into_inner(parent, up_key, new_node);
        new_node = n;
        up_key = u;
    }

    // Create new root
    if (new_node != nullptr)
    {
        this->install_new_root_node(_root, new_node, up_key);
    }
}

template <typename K, typename V, bool U>
std::pair<BPlusTreeNode<K, V, U> *, K> BPlusTree<K, V, U>::insert_into_inner(BPlusTree<K, V, U>::Node *inner_node,
                                                                             const K key,
                                                                             BPlusTree<K, V, U>::Node *separator)
{
    if (inner_node->is_full() == false)
    {
        const size_type index = inner_node->index(key);
        inner_node->insert_separator(index, separator, key);
        return {static_cast<Node *>(nullptr), 0};
    }
    else
    {
        return this->split_inner_node(inner_node, key, separator);
    }
}

template <typename K, typename V, bool U>
void BPlusTree<K, V, U>::install_new_root_node(BPlusTree<K, V, U>::Node *left, BPlusTree<K, V, U>::Node *right,
                                               const K key)
{
    Node *new_root = new Node(false);
    new_root->separator(0, left);
    new_root->insert_separator(0, right, key);
    _height++;
    _root = new_root;
}

template <typename K, typename V, bool U>
std::pair<BPlusTreeNode<K, V, U> *, K> BPlusTree<K, V, U>::split_inner_node(BPlusTree<K, V, U>::Node *inner_node,
                                                                            const K key,
                                                                            BPlusTree<K, V, U>::Node *separator)
{
    constexpr size_type left_size = BPlusTreeInnerNode<K, V, U>::max_keys / 2;
    constexpr size_type right_size = BPlusTreeInnerNode<K, V, U>::max_keys - left_size;

    K key_up;
    Node *new_inner_node = new Node(false);
    new_inner_node->right(inner_node->right());
    inner_node->right(new_inner_node);

    if (key < inner_node->inner_key(left_size - 1))
    {
        inner_node->copy(new_inner_node, left_size, right_size);
        new_inner_node->separator(0, inner_node->separator(left_size));
        new_inner_node->size(right_size);

        key_up = inner_node->inner_key(left_size - 1);
        inner_node->size(left_size - 1);

        const size_type index = inner_node->index(key);
        inner_node->insert_separator(index, separator, key);
    }
    else if (key < inner_node->inner_key(left_size))
    {
        inner_node->copy(new_inner_node, left_size, right_size);
        new_inner_node->separator(0, separator);
        key_up = key;
        inner_node->size(left_size);
        new_inner_node->size(right_size);
    }
    else
    {
        inner_node->copy(new_inner_node, left_size + 1, right_size - 1);
        new_inner_node->separator(0, inner_node->separator(left_size + 1));
        inner_node->size(left_size);
        new_inner_node->size(right_size - 1);
        key_up = inner_node->inner_key(left_size);

        const size_type index = new_inner_node->index(key);
        new_inner_node->insert_separator(index, separator, key);
    }

    return {new_inner_node, key_up};
}

template <typename K, typename V, bool U>
BPlusTreeNode<K, V, U> *BPlusTree<K, V, U>::split_leaf_node(BPlusTree<K, V, U>::Node *leaf_node)
{
    constexpr size_type left_size = BPlusTreeLeafNode<K, V, U>::max_items / 2;
    constexpr size_type right_size = BPlusTreeLeafNode<K, V, U>::max_items - left_size;

    Node *new_leaf_node = new Node(true);
    new_leaf_node->right(leaf_node->right());
    leaf_node->right(new_leaf_node);

    leaf_node->copy(new_leaf_node, left_size, right_size);
    new_leaf_node->size(right_size);
    leaf_node->size(left_size);

    return new_leaf_node;
}
} // namespace beedb::index::bplustree

#include "b_plus_tree.hpp"
