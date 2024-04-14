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
#include "date.h"
#include <cstdint>
#include <string>

namespace beedb::table
{
/**
 * Represents a database type.
 */
class Type
{
  public:
    /**
     * Ids for types.
     */
    enum Id : std::uint16_t
    {
        INT,
        LONG,
        DECIMAL,
        CHAR,
        DATE,
        UNDEFINED
    };

    [[nodiscard]] static Type make_int() noexcept
    {
        return Type{Id::INT};
    }
    [[nodiscard]] static Type make_long() noexcept
    {
        return Type{Id::LONG};
    }
    [[nodiscard]] static Type make_decimal() noexcept
    {
        return Type{Id::DECIMAL};
    }
    [[nodiscard]] static Type make_char(const std::uint16_t length) noexcept
    {
        return Type{Id::CHAR, length};
    }
    [[nodiscard]] static Type make_date() noexcept
    {
        return Type{Id::DATE};
    }

    constexpr Type() : _id(Id::UNDEFINED)
    {
    }

    constexpr Type(const Id id, const std::uint16_t length = 0u) : _id(id), _length(length)
    {
    }

    constexpr Type(const Type &other) = default;

    ~Type() = default;

    /**
     * @return Length of the type if the length is dynamic (like char).
     */
    [[nodiscard]] std::uint16_t dynamic_length() const
    {
        return _length;
    }

    operator Id() const
    {
        return _id;
    }
    bool operator==(Id id) const
    {
        return _id == id;
    }
    bool operator!=(Id id) const
    {
        return _id != id;
    }

    Type &operator=(const Id id)
    {
        _id = id;
        return *this;
    }

    Type &operator=(const Type &other) = default;

    /**
     * @return Real size in bytes of the type.
     */
    [[nodiscard]] std::uint16_t size() const
    {
        switch (_id)
        {
        case INT:
            return sizeof(std::int32_t);
        case LONG:
            return sizeof(std::int64_t);
        case CHAR:
            return sizeof(std::int8_t) * dynamic_length();
        case DECIMAL:
            return sizeof(double);
        case DATE:
            return sizeof(Date);
        case UNDEFINED:
            return 0;
        }
    }

    /**
     * @return Name of the type.
     */
    [[nodiscard]] std::string name() const
    {
        switch (_id)
        {
        case INT:
            return "INT";
        case LONG:
            return "LONG";
        case DECIMAL:
            return "DECIMAL";
        case DATE:
            return "DATE";
        case CHAR:
            return "CHAR(" + std::to_string(std::int32_t(dynamic_length())) + ")";
        case UNDEFINED:
            return "UNDEFINED";
        }
    }

  private:
    Id _id;
    std::uint16_t _length{0U};
} __attribute__((packed));
} // namespace beedb::table
