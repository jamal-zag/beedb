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
#include "b_plus_tree/non_unique_b_plus_tree_index.h"
#include "b_plus_tree/unique_b_plus_tree_index.h"
#include "index_interface.h"
#include "type.h"
#include <memory>

namespace beedb::index
{
class IndexFactory
{
  public:
    /**
     * Builds an empty index.
     *
     * @param name Name for the index.
     * @param type The index type identifies the underlying data structure like BTree, Hashtable, ...
     * @param is_unique Identifies whether the index can hold multiple values for one key.
     * @return Pointer to the in-memory index structure.
     */
    static std::shared_ptr<IndexInterface> new_index(const std::string &name, const Type type, const bool is_unique)
    {
        if (type == Type::BTree)
        {
            if (is_unique)
            {
                return std::make_shared<bplustree::UniqueBPlusTreeIndex>(name);
            }
            else
            {
                return std::make_shared<bplustree::NonUniqueBPlusTreeIndex>(name);
            }
        }

        return {};
    }
};
} // namespace beedb::index