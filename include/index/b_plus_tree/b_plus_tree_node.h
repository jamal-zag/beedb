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
#include <array>
#include <config.h>
#include <cstdint>
#include <cstring>
#include <index/return_value.h>
#include <utility>

namespace beedb::index::bplustree
{
using size_type = std::size_t;

template <typename K, typename V, bool U> class BPlusTreeNode;

template <typename K, typename V, bool U> struct BPlusTreeNodeHeader
{
    size_type size = 0;
    bool is_leaf;
    BPlusTreeNode<K, V, U> *right = nullptr;

    explicit BPlusTreeNodeHeader(const bool is_leaf_) : is_leaf(is_leaf_)
    {
    }
};

template <typename K, typename V, bool U> struct BPlusTreeLeafNode
{
    static constexpr size_type max_items = (Config::b_plus_tree_page_size - sizeof(BPlusTreeNodeHeader<K, V, U>)) /
                                           (sizeof(K) + sizeof(typename ReturnValue<V, U>::type));

    std::array<K, BPlusTreeLeafNode::max_items> keys;
    std::array<typename ReturnValue<V, U>::type, BPlusTreeLeafNode::max_items> values;
};

template <typename K, typename V, bool U> struct BPlusTreeInnerNode
{
    static constexpr size_type max_keys =
        (Config::b_plus_tree_page_size - sizeof(BPlusTreeNodeHeader<K, V, U>) - sizeof(BPlusTreeNode<K, V, U> *)) /
        (sizeof(K) + sizeof(BPlusTreeInnerNode<K, V, U> *));
    static constexpr size_type max_separators = max_keys + 1;

    std::array<K, BPlusTreeInnerNode::max_keys> keys;
    std::array<BPlusTreeNode<K, V, U> *, BPlusTreeInnerNode::max_separators> separators;
};

template <typename K, typename V, bool U> class BPlusTreeNode
{
  public:
    explicit BPlusTreeNode(const bool is_leaf) : _header(is_leaf)
    {
    }
    ~BPlusTreeNode();

    [[nodiscard]] bool is_leaf() const
    {
        return _header.is_leaf;
    }
    [[nodiscard]] bool is_inner() const
    {
        return is_leaf() == false;
    }
    [[nodiscard]] size_type size() const
    {
        return _header.size;
    }
    void size(const size_type size)
    {
        _header.size = size;
    }
    [[nodiscard]] BPlusTreeNode<K, V, U> *right()
    {
        return _header.right;
    }
    [[nodiscard]] bool has_right() const
    {
        return _header.right != nullptr;
    }
    void right(BPlusTreeNode<K, V, U> *right)
    {
        _header.right = right;
    }

    [[nodiscard]] typename ReturnValue<V, U>::type &value(const size_type index)
    {
        return _leaf_node.values[index];
    }
    [[nodiscard]] BPlusTreeNode<K, V, U> *separator(const size_type index)
    {
        return _inner_node.separators[index];
    }
    void separator(const size_type index, BPlusTreeNode<K, V, U> *separator)
    {
        _inner_node.separators[index] = separator;
    }

    [[nodiscard]] K leaf_key(const size_type index)
    {
        return _leaf_node.keys[index];
    }
    [[nodiscard]] K inner_key(const size_type index)
    {
        return _inner_node.keys[index];
    }

    [[nodiscard]] bool is_full() const
    {
        const size_type max_size =
            is_leaf() ? BPlusTreeLeafNode<K, V, U>::max_items : BPlusTreeInnerNode<K, V, U>::max_keys;
        return size() >= max_size;
    }

    size_type index(const K key);
    BPlusTreeNode<K, V, U> *child(const K key);

    void insert_separator(const size_type index, BPlusTreeNode<K, V, U> *separator, const K key);
    void insert_value(const size_type index, const V value, const K key);
    void copy(BPlusTreeNode *other, const size_type from_index, const size_type count);

    std::pair<std::size_t, std::size_t> size_include_children();
    std::pair<std::size_t, std::size_t> count_children();

  private:
    BPlusTreeNodeHeader<K, V, U> _header;

    union {
        BPlusTreeInnerNode<K, V, U> _inner_node;
        BPlusTreeLeafNode<K, V, U> _leaf_node;
    };
};

template <typename K, typename V, bool U> BPlusTreeNode<K, V, U>::~BPlusTreeNode()
{
    if (is_leaf() == false)
    {
        for (size_type i = 0; i < size(); i++)
        {
            delete _inner_node.separators[i];
        }
    }
}

template <typename K, typename V, bool U> size_type BPlusTreeNode<K, V, U>::index(const K key)
{
    auto keys = is_leaf() ? _leaf_node.keys.begin() : _inner_node.keys.begin();
    auto iterator = std::lower_bound(keys, keys + size(), key);

    return std::distance(keys, iterator);
}

template <typename K, typename V, bool U> BPlusTreeNode<K, V, U> *BPlusTreeNode<K, V, U>::child(const K key)
{
    std::int32_t low = 0, high = size() - 1;
    while (low <= high)
    {
        const std::int32_t mid = (low + high) / 2;
        if (_inner_node.keys[mid] <= key)
        {
            low = mid + 1;
        }
        else
        {
            high = mid - 1;
        }
    }

    return _inner_node.separators[high + 1];
}

template <typename K, typename V, bool U>
void BPlusTreeNode<K, V, U>::insert_separator(const size_type index, BPlusTreeNode<K, V, U> *separator, const K key)
{
    if (index < size())
    {
        const size_type offset = size() - index;
        std::memmove(&_inner_node.keys[index + 1], &_inner_node.keys[index], offset * sizeof(K));
        std::memmove(&_inner_node.separators[index + 2], &_inner_node.separators[index + 1],
                     offset * sizeof(BPlusTreeNode<K, V, U> *));
    }

    _inner_node.keys[index] = key;
    _inner_node.separators[index + 1] = separator;
    _header.size++;
}

template <typename K, typename V, bool U>
void BPlusTreeNode<K, V, U>::insert_value(const size_type index, const V value, const K key)
{
    if (index < size())
    {
        const size_type offset = size() - index;
        std::memmove(&_leaf_node.keys[index + 1], &_leaf_node.keys[index], offset * sizeof(K));
        std::memmove(static_cast<void *>(&_leaf_node.values[index + 1]), &_leaf_node.values[index],
                     offset * sizeof(typename ReturnValue<V, U>::type));
    }

    _leaf_node.keys[index] = key;

    if constexpr (U)
    {
        _leaf_node.values[index] = value;
    }
    else
    {
        new (&_leaf_node.values[index]) typename ReturnValue<V, U>::type();
        _leaf_node.values[index].insert(value);
    }

    _header.size++;
}

template <typename K, typename V, bool U>
void BPlusTreeNode<K, V, U>::copy(BPlusTreeNode<K, V, U> *other, const size_type from_index, const size_type count)
{
    if (is_leaf())
    {
        std::memcpy(&other->_leaf_node.keys[0], &_leaf_node.keys[from_index], count * sizeof(K));
        std::memcpy(static_cast<void *>(&other->_leaf_node.values[0]), &_leaf_node.values[from_index],
                    count * sizeof(typename ReturnValue<V, U>::type));
    }
    else
    {
        std::memcpy(&other->_inner_node.keys[0], &_inner_node.keys[from_index], count * sizeof(K));
        std::memcpy(&other->_inner_node.separators[1], &_inner_node.separators[from_index + 1],
                    count * sizeof(BPlusTreeNode<K, V, U> *));
    }
}

template <typename K, typename V, bool U>
std::pair<std::size_t, std::size_t> BPlusTreeNode<K, V, U>::size_include_children()
{
    if (is_leaf())
    {
        return {0u, size()};
    }

    std::size_t leaf_sizes = 0, inner_sizes = 0;
    for (auto i = 0u; i <= size(); i++)
    {
        BPlusTreeNode<K, V, U> *child = _inner_node.separators[i];
        const auto child_size = child->size_include_children();
        inner_sizes += child_size.first;
        leaf_sizes += child_size.second;
    }

    return {inner_sizes, leaf_sizes};
}

template <typename K, typename V, bool U> std::pair<std::size_t, std::size_t> BPlusTreeNode<K, V, U>::count_children()
{
    if (is_leaf())
    {
        return {0u, 0u};
    }

    if (_inner_node.separators[0]->is_leaf())
    {
        return {0u, size() + 1u};
    }

    std::size_t leaf_children = 0, inner_children = 0;
    for (auto i = 0u; i <= size(); i++)
    {
        BPlusTreeNode<K, V, U> *child = _inner_node.separators[i];

        const auto child_size = child->count_children();
        inner_children += child_size.first;
        leaf_children += child_size.second;
    }

    return {inner_children + size(), leaf_children};
}
} // namespace beedb::index::bplustree
