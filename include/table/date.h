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
#include <cstdint>
#include <functional>
#include <regex>
#include <string>

namespace beedb::table
{
/**
 * Implements date type.
 */
class Date
{
  public:
    Date() noexcept = default;
    Date(const std::uint16_t year, const std::uint8_t month, const std::uint8_t day) noexcept
        : _year(year), _month(month), _day(day)
    {
    }
    Date(const Date &) = default;

    ~Date() noexcept = default;

    [[nodiscard]] std::uint16_t year() const noexcept
    {
        return _year;
    }
    [[nodiscard]] std::uint8_t month() const noexcept
    {
        return _month;
    }
    [[nodiscard]] std::uint8_t day() const noexcept
    {
        return _day;
    }

    Date &operator=(const Date &other) = default;

    bool operator==(const Date other) const noexcept
    {
        return _year == other._year && _month == other._month && _day == other._day;
    }

    bool operator!=(const Date other) const noexcept
    {
        return _year != other._year || _month != other._month || _day != other._day;
    }

    bool operator<(const Date other) const noexcept
    {
        return _year < other._year || (_year == other._year && _month < other._month) ||
               (_year == other._year && _month == other._month && _day < other._day);
    }

    bool operator<=(const Date other) const noexcept
    {
        return _year < other._year || (_year == other._year && _month < other._month) ||
               (_year == other._year && _month == other._month && _day <= other._day);
    }

    bool operator>(const Date other) const noexcept
    {
        return _year > other._year || (_year == other._year && _month > other._month) ||
               (_year == other._year && _month == other._month && _day > other._day);
    }

    bool operator>=(const Date other) const noexcept
    {
        return _year > other._year || (_year == other._year && _month > other._month) ||
               (_year == other._year && _month == other._month && _day >= other._day);
    }

    bool operator==(std::nullptr_t) const noexcept
    {
        return _year == 0 && _month == 0 && _day == 0;
    }

    [[nodiscard]] std::string to_string() const
    {
        return std::to_string(static_cast<std::int16_t>(_year)) + "-" +
               std::to_string(static_cast<std::int16_t>(_month)) + "-" +
               std::to_string(static_cast<std::int16_t>(_day));
    }

    static Date from_string(const std::string &date_string)
    {
        std::smatch match;
        if (std::regex_match(date_string, match, _date_regex))
        {
            return Date{static_cast<std::uint16_t>(std::stoul(match[1].str())),
                        static_cast<std::uint8_t>(std::stoul(match[2].str())),
                        static_cast<std::uint8_t>(std::stoul(match[3].str()))};
        }

        return Date{};
    }

  private:
    inline static std::regex _date_regex{"(\\d{4})-(\\d{2})-(\\d{2})"};
    std::uint16_t _year = 0;
    std::uint8_t _month = 0;
    std::uint8_t _day = 0;
};
} // namespace beedb::table

namespace std
{
template <> struct hash<beedb::table::Date>
{
    std::size_t operator()(const beedb::table::Date &date)
    {
        const std::uint32_t h = ((((std::uint32_t{0} | date.year()) << 8) | date.month()) << 8) || date.day();
        return hash<std::uint32_t>()(h);
    }
};
} // namespace std