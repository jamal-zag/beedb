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

    // 1. "Start the traversal at the root node."
    //    (This assumes _root is a direct pointer to the root node in memory)
    Node *current_node = this->_root;

    // 2. Loop "while 'current_node' is not a leaf."
    //    'is_inner()' is the opposite of 'is_leaf()'.
    while (current_node->is_inner())
    {
        // 3. "Push every node to the 'node_path', if 'node_path != nullptr'."
        //    This is used by 'insert' to trace its path for potential splits.
        if (node_path)
        {
            node_path->push_back(current_node);
        }

        // 4. "Get the next node using 'current_node->child(key)'..."
        //    This helper method finds the correct child pointer for the 'key'
        //    and returns the child node to continue the traversal.
        current_node = current_node->child(key);
    }

    // 5. "Return the leaf you found during traversal."
    //    The loop has terminated, meaning 'current_node->is_inner()' was false,
    //    so 'current_node' must be the correct leaf.
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

    // 1. Locate the leaf node
    auto *leaf = this->locate_leaf(key);

    // 2. Check if the tree is empty (leaf not found)
    if (leaf == nullptr)
    {
        // Return an empty optional of the correct type
        if constexpr (U) {
            return std::optional<V>{std::nullopt};
        } else {
            return std::optional<std::set<V>>{std::nullopt};
        }
    }

    // 3. Find the index for the key
    const auto index = leaf->index(key);

    // 4. Check if the index is valid AND the key is an exact match
    if (index < leaf->size() && leaf->leaf_key(index) == key)
    {
        // Key found. The value() method will return V or std::set<V>
        // based on U. We wrap it in std::make_optional to return.
        return std::make_optional(leaf->value(index));
    }
    else
    {
        // Key not found in the leaf
        if constexpr (U) {
            return std::optional<V>{std::nullopt};
        } else {
            return std::optional<std::set<V>>{std::nullopt};
        }
    }

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

    // A std::set is used to collect all unique values (V) found.
    std::set<V> values;

    // 1. "Locate the leaf node that may contain the wanted key."
    //    We start by finding the leaf where 'key_from' would be.
    auto *leaf = this->locate_leaf(key_from);

    // 2. Start a loop that will scan horizontally (leaf by leaf)
    //    as long as 'leaf' points to a valid node.
    while (leaf != nullptr)
    {
        // 3. Find the starting position within the current leaf.
        //    'leaf->index(key_from)' gives the index of the first key >= 'key_from'.
        // 4. Iterate from that starting index to the end of the *current* leaf's keys.
        for (auto i = leaf->index(key_from); i < leaf->size(); i++)
        {
            // 5. Get the key at the current index.
            const auto key = leaf->leaf_key(i);

            // 6. "Add all keys that are equal or greater than the key 'key_from'
            //    and equal or lesser than the key 'key_to'..."
            //    (Note: This code implements key < key_to, not <= key_to)
            if (key >= key_from && key < key_to)
            {
                // 7. The key is in range. Add its value(s) to the set.
                //    We must use 'if constexpr' to handle the two
                //    different tree types (Unique vs. Non-Unique).

                if constexpr (U) // 'U' is true (Unique tree)
                {
                    // For a unique tree, 'leaf->value(i)' returns a single value 'V'.
                    values.insert(leaf->value(i));
                }
                else // 'U' is false (Non-Unique tree)
                {
                    // For a non-unique tree, 'leaf->value(i)' returns a 'std::set<V>'.
                    auto value_set = leaf->value(i);
                    // We must add all values from that set into our main 'values' set.
                    values.insert(value_set.begin(), value_set.end());
                }
            }
            // (If key >= key_to, we just continue iterating until the
            //  end of this leaf, as an earlier key might have been < key_to)
        }

        // 8. "When the last key ... matches ... take a look to the right neighbour"
        //    We check the *last key* in this leaf. If it's still less than
        //    'key_to', the range might continue. We also must check
        //    that a 'right()' neighbor actually exists.
        if (leaf->leaf_key(leaf->size() - 1) < key_to && leaf->right() != nullptr)
        {
            // 9. Move to the next leaf node to continue the scan.
            leaf = leaf->right();
        }
        else
        {
            // 10. Stop scanning. This happens if:
            //     a) The last key in this leaf was >= 'key_to', so no further keys can be in the range.
            //     b) We are at the right-most leaf (leaf->right() is null).
            leaf = nullptr;
        }
    }

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

    // 1. "Every leaf node provides a method 'index(k)'..."
    //    Find the index where the key *should* be, or where it *is* if it exists.
    const auto index = leaf_node->index(key);

    // 2. "Check if the leaf node already contains the key"
    //    We do this by checking if the key at the found 'index' is an exact match.
    //    (We must also check index < size() to avoid reading past the end)
    if (index < leaf_node->size() && leaf_node->leaf_key(index) == key)
    {
        // 3. "If yes and the tree is non-unique: add the value..."
        //    We use 'if constexpr' to check the 'U' (Unique) template parameter.
        if constexpr (U == false) // 'U' is false (Non-Unique tree)
        {
            // 'leaf_node->value(index)' returns a std::set<V>. We add the new value.
            leaf_node->value(index).insert(value);
        }
        // 4. "If yes and the tree is unique: Just return a 'nullptr'."
        //    (If 'U' is true, we do nothing because duplicates are not allowed).
    }
    // 5. "If the key is not in the node, check for space..."
    //    The key is new. Check if the node is *not* full.
    else if (leaf_node->is_full() == false)
    {
        // 6. "If the node is not full, insert the new pair..."
        //    The 'insert_value' helper adds the (key, value) pair at the correct 'index'.
        leaf_node->insert_value(index, value, key);
    }
    // 7. "Otherwise, we have to split the node."
    //    The key is new, but the node is full.
    else
    {
        // 8. Call the split helper. This modifies 'leaf_node' (left half)
        //    and returns a pointer to the new right sibling.
        auto *right_leaf = this->split_leaf_node(leaf_node);

        // 9. "After splitting, ... Check whether the key should take place
        //    in the given leaf or the new leaf..."
        //    We compare our 'key' to the *first key* in the *new* right node.
        if (key < right_leaf->leaf_key(0))
        {
            // 10. The key belongs in the original (left) node.
            //     We find its new index (since the node changed) and insert.
            leaf_node->insert_value(leaf_node->index(key), value, key);
        }
        else
        {
            // 11. The key belongs in the new (right) node.
            //     We find its index *in that new node* and insert.
            right_leaf->insert_value(right_leaf->index(key), value, key);
        }

        // 12. "After splitting, return the pointer to the new leaf."
        //     This return value signals to the calling 'insert' function
        //     that a split occurred and an internal node must be updated.
        return right_leaf;
    }

    // 13. If we are here, we either inserted into a non-full node,
    //     or added to a non-unique key, or did nothing for a unique key.
    //     In all cases, no split occurred, so we return 'nullptr'.
    return nullptr;
}
} // namespace beedb::index::bplustree
