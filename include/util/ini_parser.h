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
#include <string>
#include <unordered_map>
#include <utility>

namespace beedb::util
{
/**
 * Parses an ini file and stores the parsed key,value tuples
 * including sections.
 */
class IniParser
{
  public:
    using key_t = std::pair<std::string, std::string>;
    using value_t = std::string;

    /**
     * Initializes the parser and parses the given
     * .ini file.
     *
     * @param file_name
     */
    explicit IniParser(const std::string &file_name);

    ~IniParser() = default;

    /**
     * @return True, if the parsed config is empty
     *  (maybe because the file was not found).
     */
    [[nodiscard]] bool empty() const
    {
        return _configurations.empty();
    }

    /**
     * Returns the value for a given
     * key in a given section or the
     * default value of the key does
     * not exists.
     *
     * @param default_value
     * @return Value of the key or default value.
     */
    template <typename T> T get(const std::string &, const std::string &, const T &default_value) const
    {
        return default_value;
    }

  private:
    struct key_t_hash
    {
        std::size_t operator()(const key_t &pair) const
        {
            return std::hash<std::string>()(pair.first) ^ std::hash<std::string>()(pair.second);
        }
    };

    std::unordered_map<key_t, value_t, key_t_hash> _configurations;

    /**
     * Parses the ini file with the given name.
     *
     * @param file_name Name of the ini file.
     */
    void parse(const std::string &file_name);
};
} // namespace beedb::util

template <>
inline std::string beedb::util::IniParser::get(const std::string &section, const std::string &key,
                                               const std::string &default_value) const
{
    const auto config_key = key_t{std::make_pair(section, key)};
    if (this->_configurations.find(config_key) == this->_configurations.end())
    {
        return default_value;
    }

    return this->_configurations.at(config_key);
}

template <>
inline bool beedb::util::IniParser::get(const std::string &section, const std::string &key,
                                        const bool &default_value) const
{
    const auto config_key = key_t{std::make_pair(section, key)};
    if (this->_configurations.find(config_key) == _configurations.end())
    {
        return default_value;
    }

    return bool(std::stoi(this->_configurations.at(config_key)));
}

template <>
inline std::int32_t beedb::util::IniParser::get(const std::string &section, const std::string &key,
                                                const std::int32_t &default_value) const
{
    const auto config_key = key_t{std::make_pair(section, key)};
    if (this->_configurations.find(config_key) == this->_configurations.end())
    {
        return default_value;
    }

    return std::int32_t(std::stol(this->_configurations.at(config_key)));
}

template <>
inline std::uint32_t beedb::util::IniParser::get(const std::string &section, const std::string &key,
                                                 const std::uint32_t &default_value) const
{
    const auto config_key = key_t{std::make_pair(section, key)};
    if (this->_configurations.find(config_key) == this->_configurations.end())
    {
        return default_value;
    }

    return std::uint32_t(std::stoul(this->_configurations.at(config_key)));
}

template <>
inline std::int64_t beedb::util::IniParser::get(const std::string &section, const std::string &key,
                                                const std::int64_t &default_value) const
{
    const auto config_key = key_t{std::make_pair(section, key)};
    if (this->_configurations.find(config_key) == this->_configurations.end())
    {
        return default_value;
    }

    return std::int64_t(std::stoll(this->_configurations.at(config_key)));
}

template <>
inline std::uint64_t beedb::util::IniParser::get(const std::string &section, const std::string &key,
                                                 const std::uint64_t &default_value) const
{
    const auto config_key = key_t{std::make_pair(section, key)};
    if (this->_configurations.find(config_key) == this->_configurations.end())
    {
        return default_value;
    }

    return std::uint64_t(std::stoull(this->_configurations.at(config_key)));
}