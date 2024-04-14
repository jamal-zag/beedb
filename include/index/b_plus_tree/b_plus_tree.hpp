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

#include <index/b_plus_tree/b_plus_tree_node.h>
#include <index/return_value.h>
#include <iostream>
#include <optional>
#include <utility>
#include <vector>

namespace beedb::index::bplustree
{
template <typename K, typename V, bool U>
BPlusTreeNode<K, V, U> *BPlusTree<K, V, U>::locate_leaf(
    [[maybe_unused]] const K key, [[maybe_unused]] std::vector<BPlusTreeNode<K, V, U> *> *node_path) const
{
    /**
     * Assignment (2): Implement a B+-Tree
     *
     * The B+-Tree is used for indexing files. Using the index
     * for bigger data sets will reduce the amount of scanned disk pages
     * in case the query wants to filter the data.
     *
     * This method is used to traverse the tree and locate a leaf that may
     * contain the wanted key. During the traversal for inserts, all visited nodes
     * are stored in the node_path container. For lookups, the container is null.
     *
     * Hints for implementation:
     *  - "this->_root" stores the root node of the tree, where every
     *    lookup for a leaf is started.
     *  - Every inner node (also the root) has a "child(k)" method, which returns
     *    the next node on the way for the leaf, that may contain the key "k".
     *  - To check a node whether it is a leaf or inner node, use the "is_leaf()"
     *    method on a node, which returns true if the node is a leaf.
     *  - If "node_path" is not a "nullptr", push all nodes during traversal
     *    to that vector (also the root).
     *
     *
     * Procedure:
     *  - Start the traversal at the root node.
     *  - Get the next node using "current_node->child(key)" while "current_node"
     *    is not a leaf.
     *  - Push every node to the "node_path", if "node_path != nullptr".
     *  - Return the leaf you found during traversal.
     */

    Node *current_node = this->_root;

    // TODO: Insert your code here.

    return current_node;
}

template <typename K, typename V, bool U>
std::optional<typename ReturnValue<V, U>::type> BPlusTree<K, V, U>::get([[maybe_unused]] const K key) const
{
    /**
     * Assignment (2): Implement a B+-Tree
     *
     * This method tries to find the value for a given key.
     * The tree is a very generic data structure which can hold
     * one value per key or multiple values per key. The specific
     * variant is given by the template parameter U which is a bool
     * and stands for Unique. True means: Return one value of type V;
     * false means: Return a set of values of type V.
     *
     * Hints for implementation:
     *  - You have already implemented "locate_leaf(k)" which returns the
     *    leaf that may contain the searched key-value pair.
     *  - Every leaf node provides a method "index(k)" which returns the index
     *    of the key "k".
     *  - Every leaf node provides a method "leaf_key(i)" which returns the
     *    key at index "i".
     *  - Every leaf node provides a method "value(i)" which returns the value
     *    at index "i". The "value(i)" method will automatically pick the correct
     *    return type, depending on the U-template-parameter.
     *
     * Procedure:
     *  - Locate the leaf node that may contain the wanted key.
     *  - Check the leaf node: Is the wanted key available?
     *  - If yes: return the value of the key.
     *  - Otherwise return an empty result, using "return std::nullopt;".
     */

    // TODO: Insert your code here.

    return std::nullopt;
}

template <typename K, typename V, bool U>
std::optional<std::set<V>> BPlusTree<K, V, U>::get([[maybe_unused]] const K key_from,
                                                   [[maybe_unused]] const K key_to) const
{
    /**
     * Assignment (2): Implement a B+-Tree
     *
     * This method tries to find one or multiple values for a given
     * range of keys.
     * The tree is a very generic data structure which can hold
     * one value per key or multiple values per key. The specific
     * variant is given by the template parameter U which is a bool
     * and stands for Unique. True means: Return one value of type V;
     * false means: Return a set of values of type V.
     *
     * Hints for implementation:
     *  - You have already implemented "locate_leaf(k)" which returns the
     *    leaf that may contain the searched key-value pair.
     *  - Every node provides a method "right()" which returns a pointer
     *    to the right neighbour node.
     *  - Every node provides a method "size()" which returns the number of
     *    items that are stored in the node.
     *  - Every leaf node provides a method "index(k)" which returns the index
     *    of the key "k".
     *  - Every leaf node provides a method "leaf_key(i)" which returns the
     *    key at index "i".
     *  - Every leaf node provides a method "value(i)" which returns the value
     *    at index "i". The "value(i)" method will automatically pick the correct
     *    return type, depending on the U-template-parameter.
     *  - You can test whether it is a unique or non-unique tree, using
     *    "if constexpr(U) { code for unique... } else { boot for non-unique... }.
     *  - Both containers std::set (https://en.cppreference.com/w/cpp/container/set)
     *    and std::optional (https://en.cppreference.com/w/cpp/utility/optional) may
     *    be helpful on compiler errors.
     *
     * Procedure:
     *  - Locate the leaf node that may contain the wanted key.
     *  - Add all keys that are equal or greater than the key "key_from"
     *    and equal or lesser than the key "key_to" to a set of values.
     *  - When the last key of the node matches that predicate, also
     *    take a look to the right neighbour using the "right()" method
     *    (and also the rights right,...).
     */

    std::set<V> values;

    // TODO: Insert your code here.

    return values;
}

template <typename K, typename V, bool U>
BPlusTreeNode<K, V, U> *BPlusTree<K, V, U>::insert_into_leaf([[maybe_unused]] BPlusTreeNode<K, V, U> *leaf_node,
                                                             [[maybe_unused]] const K key,
                                                             [[maybe_unused]] const V value)
{
    /**
     * Assignment (2): Implement a B+-Tree
     *
     * This method adds a value to a leaf node. The correct leaf node, key and
     * value are all given. When inserting results in splitting the leaf,
     * the pointer to the new created node is returned.
     *
     * Hints for implementation:
     *  - Every node provides a method "full()" which returns true, if there
     *    is no more place for a new item.
     *  - Every leaf node provides a method "index(k)" which returns the index
     *    of the key "k".
     *  - Every leaf node provides a method "leaf_key(i)" which returns the
     *    key at index "i".
     *  - Every leaf node provides a method "insert_value(i, v, k)" which adds
     *    a key-value pair (k,v) to the leaf at index i.
     *  - The tree has a method "this->split_leaf_node(l)" which splits the leaf
     *    node l and returns a pointer to the new node.
     *  - You can test whether it is a unique or non-unique tree, using
     *    "if constexpr(U) { code for unique... } else { boot for non-unique... }.
     *
     * Procedure:
     *  - Check if the leaf node already contains the key
     *      - If yes and the tree is non-unique: add the value to the list of values
     *        in the node and return a "nullptr".
     *      - If yey and the tree is unique: Just return a "nullptr".
     *  - If the key is not in the node, check for space for a new (key,value) pair.
     *  - If the node is not full, insert the new pair and return a "nullptr"
     *  - Otherwise, we have to split the node. Splitting will create a new leaf node,
     *    the new right neighbour of the given leaf node.
     *  - After splitting, we have enough space to insert the pair. Check whether the key
     *    should take place in the given leaf or the new leaf, created on splitting:
     *    When the key is lower than the first key of the new leaf, the key should be insert
     *    into the given leaf, otherwise in the new leaf.
     *  - After splitting, return the pointer to the new leaf.
     */

    // TODO: Insert your code here.

    return nullptr;
}
} // namespace beedb::index::bplustree
