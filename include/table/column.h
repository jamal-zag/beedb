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
#include "type.h"
#include <index/index_interface.h>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

namespace beedb::table
{
/**
 * Represents one column in a table schema.
 */
class Column
{
    friend std::ostream &operator<<(std::ostream &stream, const Column &column);

  public:
    Column() = default;

    Column(const std::int32_t id, const Type type, const bool is_nullable = false,
           std::vector<std::shared_ptr<index::IndexInterface>> &&indices = {})
        : _id(id), _type(type), _is_nullable(is_nullable), _indices(indices)
    {
    }

    explicit Column(const Type type, const bool is_nullable = false,
                    std::vector<std::shared_ptr<index::IndexInterface>> &&indices = {})
        : _type(type), _is_nullable(is_nullable), _indices(indices)
    {
    }

    Column(const Column &other) = default;

    Column(Column &&) = default;

    Column &operator=(Column &&) = default;

    ~Column() = default;

    /**
     * @return Id of the column or -1 if the column was not persisted to the metadata table.
     */
    [[nodiscard]] std::int32_t id() const
    {
        return _id;
    }

    /**
     * @return Type of the column.
     */
    [[nodiscard]] const Type &type() const
    {
        return _type;
    }

    /**
     * @return True, if data can be null for the column.
     */
    [[nodiscard]] bool is_nullable() const
    {
        return _is_nullable;
    }

    /**
     * @return True, when at least one index exists for this column.
     */
    [[nodiscard]] bool is_indexed() const
    {
        return _indices.empty() == false;
    }

    /**
     * Allows to ask for an index with specific requirements.
     *
     * @param require_range_index Specifies if we need an index supporting range queries.
     * @return True, when an index with given requirements exists.
     */
    [[nodiscard]] bool is_indexed(const bool require_range_index) const
    {
        if (require_range_index == false)
        {
            return is_indexed();
        }

        for (auto &index : _indices)
        {
            if (index->supports_range())
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Adds an index to the column.
     *
     * @param index Index to add.
     */
    inline void add_index(std::shared_ptr<index::IndexInterface> index)
    {
        _indices.push_back(std::move(index));
    }

    /**
     * @return All indices for this column.
     */
    [[nodiscard]] const std::vector<std::shared_ptr<index::IndexInterface>> &indices() const
    {
        return _indices;
    }

    /**
     * Checks whether an index with a specific name exists.
     *
     * @param name Name of the index.
     * @return True, when the index exists.
     */
    [[nodiscard]] bool has_index(const std::string &name) const
    {
        return index(name) != nullptr;
    }

    /**
     * Lookup for a specific index.
     *
     * @param name  Name of the index.
     * @return Pointer to the specific index.
     */
    [[nodiscard]] std::shared_ptr<index::IndexInterface> index(const std::string &name) const
    {
        for (auto &index : _indices)
        {
            if (index->name() == name)
            {
                return index;
            }
        }

        return {};
    }

    /**
     * Lookup for a specific index.
     *
     * @param need_range Specifies whether the wanted index has to provied range queries.
     * @return An index that supports the requirements.
     */
    [[nodiscard]] std::shared_ptr<index::IndexInterface> index(const bool need_range) const
    {
        if (_indices.empty() == false && need_range == false)
        {
            return _indices[0];
        }

        for (auto &index : _indices)
        {
            if (index->supports_range())
            {
                return index;
            }
        }

        return {};
    }

    bool operator==(const Column &other) const
    {
        return _id == other.id();
    }
    bool operator!=(const Column &other) const
    {
        return _id != other.id();
    }
    bool operator==(const Type type) const
    {
        return _type == type;
    }
    bool operator!=(const Type type) const
    {
        return _type != type;
    }

  private:
    std::int32_t _id{-1};
    Type _type{};
    bool _is_nullable{false};
    std::vector<std::shared_ptr<index::IndexInterface>> _indices;
};
} // namespace beedb::table