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

#include "column.h"
#include "schema.h"
#include "type.h"
#include "value.h"
#include <algorithm>
#include <bitset>
#include <cassert>
#include <concurrency/metadata.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <storage/page.h>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace beedb::table
{
/**
 * Represents a tuple stored on the database.
 */
class Tuple
{
  public:
    /**
     * Creates a tuple with a given schema, stored on a page on the disk.
     *
     * @param schema Schema for the tuple.
     * @param record_identifier Identifier of the row on the disk.
     * @param metadata Metadata for concurrency.
     * @param data Raw content of the tuple.
     */
    Tuple(const Schema &schema, const storage::RecordIdentifier record_identifier, concurrency::Metadata *metadata,
          std::byte *data)
        : _schema(schema), _record_identifier(record_identifier), _metadata(metadata), _data(data)
    {
    }

    /**
     * Creates a tuple living in the memory. The tuple is not
     * persisted on the disk.
     *
     * @param schema Schema of the tuple.
     * @param row_size Size of the tuple in bytes.
     */
    Tuple(const Schema &schema, const std::size_t row_size)
        : _schema(schema), _metadata(new concurrency::Metadata(concurrency::timestamp::make_infinity())),
          _data(new std::byte[row_size])
    {
        std::memset(_data, '\0', row_size);
    }

    /**
     * Moves the data from the other tuple into this. The
     * other tuple will contain no data after moving.
     *
     * @param schema Schema for the tuple.
     * @param move_from Source tuple.
     */
    Tuple(const Schema &schema, Tuple &&move_from)
        : _schema(schema), _record_identifier(move_from._record_identifier), _metadata(move_from._metadata),
          _data(move_from._data)
    {
        move_from._metadata = nullptr;
        move_from._data = nullptr;
    }

    /**
     * Moves the data from the other tuple into this. The
     * other tuple will contain no data after moving.
     *
     * @param move_from Source tuple.
     */
    Tuple(Tuple &&move_from) noexcept
        : _schema(move_from.schema()), _record_identifier(move_from._record_identifier), _metadata(move_from._metadata),
          _data(move_from._data)
    {
        move_from._metadata = nullptr;
        move_from._data = nullptr;
    }

    /**
     * Copies a tuple into memory.
     *
     * @param copy_from Source tuple.
     */
    Tuple(const Tuple &copy_from)
        : _schema(copy_from.schema()), _metadata(new concurrency::Metadata(*copy_from._metadata)),
          _data(new std::byte[copy_from._schema.row_size()])
    {
        std::memcpy(_data, copy_from._data, copy_from.schema().row_size());
    }

    /**
     * Frees the memory, if the tuple is not persisted on the disk.
     */
    ~Tuple()
    {
        if (static_cast<bool>(_record_identifier) == false && _data != nullptr)
        {
            delete _metadata;
            delete[] _data;
        }
    }

    /**
     * @return Schema of the tuple.
     */
    [[nodiscard]] const Schema &schema() const
    {
        return _schema;
    }

    /**
     * @return Id of the page the tuple is persisted on.
     */
    [[nodiscard]] storage::Page::id_t page_id() const
    {
        return _record_identifier.page_id();
    }

    /**
     * @return Offset in page or -1 if tuple is in memory.
     */
    [[nodiscard]] std::uint16_t slot_id() const
    {
        return _record_identifier.slot();
    }

    /**
     * @return Record identifier.
     */
    [[nodiscard]] storage::RecordIdentifier record_identifier() const
    {
        return _record_identifier;
    }

    /**
     * @return Access to the raw data.
     */
    [[nodiscard]] std::byte *data() const
    {
        return _data;
    }

    /**
     * @return True, when the tuple has data.
     */
    [[nodiscard]] bool has_data() const
    {
        return _data != nullptr;
    }

    /**
     * Updates the raw data.
     *
     * @param data New raw data.
     */
    void data(std::byte *data)
    {
        _data = data;
    }

    /**
     * @return Metadata for concurrency of this tuple.
     */
    [[nodiscard]] concurrency::Metadata *metadata() const
    {
        return _metadata;
    }

    /**
     * Updates the metadata pointer.
     * @param metadata New metadata.
     */
    void metadata(concurrency::Metadata *metadata)
    {
        _metadata = metadata;
    }

    /**
     * Extracts a value from the tuple for a specific column.
     *
     * @param index Index of the column.
     * @return Value of the tuple.
     */
    [[nodiscard]] Value get(const std::size_t index) const
    {
        const auto offset = _schema.offset(index);
        const Type &type = _schema[index].type();
        if (type == Type::INT)
        {
            return {type, *reinterpret_cast<std::int32_t *>(&(_data[offset]))};
        }
        else if (type == Type::LONG)
        {
            return {type, *reinterpret_cast<std::int64_t *>(&(_data[offset]))};
        }
        else if (type == Type::DECIMAL)
        {
            return {type, *reinterpret_cast<double *>(&(_data[offset]))};
        }
        else if (type == Type::CHAR)
        {
            return {type, std::string_view(reinterpret_cast<char *>(&_data[offset]))};
        }
        else if (type == Type::DATE)
        {
            return {type, *reinterpret_cast<Date *>(&(_data[offset]))};
        }

        return {type, 0};
    }

    /**
     * Set the value of the tuple at the given index to the given long.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const std::int64_t value)
    {
        const auto offset = _schema.offset(index);
        *reinterpret_cast<std::int64_t *>(&(_data[offset])) = value;
    }

    /**
     * Set the value of the tuple at the given index to the given int.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const std::int32_t value)
    {
        const auto offset = _schema.offset(index);
        *reinterpret_cast<std::int32_t *>(&(_data[offset])) = value;
    }

    /**
     * Set the value of the tuple at the given index to the given decimal.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const double value)
    {
        const auto offset = _schema.offset(index);
        *reinterpret_cast<double *>(&(_data[offset])) = value;
    }

    /**
     * Set the value of the tuple at the given index to the given date.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const Date value)
    {
        const auto offset = _schema.offset(index);
        *reinterpret_cast<Date *>(&(_data[offset])) = value;
    }

    /**
     * Set the value of the tuple at the given index to the given string.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const std::string &value)
    {
        const auto offset = _schema.offset(index);
        const auto column_size = _schema[index].type().size();
        const auto length = std::min(std::size_t(column_size), value.length());
        std::memcpy(&(_data[offset]), value.c_str(), length);
        if (value.length() < column_size)
        {
            std::memset(&(_data[offset + value.length()]), '\0', column_size - value.length());
        }
    }

    /**
     * Set the value of the tuple at the given index to the given string.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, std::string &&value)
    {
        const auto offset = _schema.offset(index);
        const auto column_size = _schema[index].type().size();
        const auto length = std::min(std::size_t(column_size), value.length());
        std::memmove(&(_data[offset]), value.c_str(), length);
        if (value.length() < column_size)
        {
            std::memset(&(_data[offset + value.length()]), '\0', column_size - value.length());
        }
    }

    /**
     * Set the value of the tuple at the given index to the given string.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const std::string_view &value)
    {
        const auto offset = _schema.offset(index);
        const auto column_size = _schema[index].type().size();
        const auto length = std::min(std::size_t(column_size), value.length());
        std::memcpy(&(_data[offset]), value.data(), length);
        if (value.length() < column_size)
        {
            std::memset(&(_data[offset + value.length()]), '\0', column_size - value.length());
        }
    }

    /**
     * Set the value of the tuple at the given index to the given string.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, std::string_view &&value)
    {
        const auto offset = _schema.offset(index);
        const auto column_size = _schema[index].type().size();
        const auto length = std::min(std::size_t(column_size), value.length());
        std::memmove(&(_data[offset]), value.data(), length);
        if (value.length() < column_size)
        {
            std::memset(&(_data[offset + value.length()]), '\0', column_size - value.length());
        }
    }

    /**
     * Set the value of the tuple at the given index to null.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, std::nullptr_t)
    {
        const auto type = _schema.column(index).type();

        if (type == Type::Id::LONG || type == Type::Id::INT || type == Type::Id::DECIMAL || type == Type::Id::DATE)
        {
            auto value = Value::make_null(type);
            set(index, std::move(value.value()));
        }
        else if (type == Type::Id::CHAR)
        {
            // TODO: What is the NULL-Value of a string?
            const auto offset = _schema.offset(index);
            *reinterpret_cast<char *>(&_data[offset]) = '\0';
        }
    }

    /**
     * Set the value of the tuple at the given index to the given value.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const Value &value)
    {
        set(index, value.value());
    }

    /**
     * Set the value of the tuple at the given index to the given value.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, const Value::value_type &value)
    {
        std::visit([this, index](const auto &v) { this->set(index, v); }, value);
    }

    /**
     * Set the value of the tuple at the given index to the given value.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, Value &&value)
    {
        set(index, std::move(value.value()));
    }

    /**
     * Set the value of the tuple at the given index to the given value.
     * @param index Index for the value.
     * @param value New value.
     */
    void set(const std::size_t index, Value::value_type &&value)
    {
        std::visit([this, index](auto &&v) { this->set(index, std::move(v)); }, value);
    }

  private:
    const Schema &_schema;
    const storage::RecordIdentifier _record_identifier;
    concurrency::Metadata *_metadata;
    std::byte *_data;
};
} // namespace beedb::table