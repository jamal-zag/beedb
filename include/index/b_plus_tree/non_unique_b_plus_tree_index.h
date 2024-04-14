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

#include "b_plus_tree.h"
#include <index/index_interface.h>
#include <index/non_unique_index_interface.h>
#include <index/range_index_interface.h>
#include <storage/page.h>

namespace beedb::index::bplustree
{
/**
 * B+-Tree implementation for non-unique (key,value) pairs.
 * Can store multiple values for each key.
 * Supports range queries.
 */
class NonUniqueBPlusTreeIndex : public IndexInterface, public NonUniqueIndexInterface, public RangeIndexInterface
{
  public:
    explicit NonUniqueBPlusTreeIndex(const std::string &name) : IndexInterface(name)
    {
    }
    ~NonUniqueBPlusTreeIndex() override = default;

    [[nodiscard]] bool supports_range() const override
    {
        return true;
    }
    [[nodiscard]] bool is_unique() const override
    {
        return false;
    }

    void put(const std::int64_t key, storage::Page::id_t page_pointer) override
    {
        _tree.put(key, page_pointer);
    }

    [[nodiscard]] std::optional<std::set<storage::Page::id_t>> get(const std::int64_t key) const override
    {
        return _tree.get(key);
    }

    [[nodiscard]] std::optional<std::set<storage::Page::id_t>> get(const std::int64_t key_from,
                                                                   const std::int64_t key_to) override
    {
        return _tree.get(key_from, key_to);
    }

  private:
    BPlusTree<std::int64_t, storage::Page::id_t, false> _tree;
};
} // namespace beedb::index::bplustree