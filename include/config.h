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
#include <cassert>
#include <cstdint>
#include <exception/config_exception.h>
#include <iostream>
#include <string>
#include <unordered_map>

namespace beedb
{
/**
 * Holds all configuration for the DBMS.
 */
class Config
{
  public:
    using ConfigKey = std::string;
    using ConfigValue = std::int32_t; // TODO: consider renaming this or the struct below...?

    ///// COMPILE TIME OPTIONS - CHANGES REQUIRE REBUILDING
    static constexpr std::uint16_t page_size = 4096;
    static constexpr std::uint16_t b_plus_tree_page_size = 1024;
    static constexpr auto max_clients = 64u;
    static constexpr auto cli_history_file = "beedb-cli.txt";

  public:
    // defining constants and other values, for convenience and ease-of-reading
    enum BufferReplacementStrategy
    {
        Random,
        LRU,
        LRU_K,
        LFU,
        Clock
    };

    // important key's for non-string based notation:
    static constexpr auto k_PageSize = "page_size";
    static constexpr auto k_BPlusTreePageSize = "b_plus_tree_page_size";
    static constexpr auto k_ScanPageLimit = "scan_page_limit";

    static constexpr auto k_BufferFrames = "buffer_frames";
    static constexpr auto k_BufferReplacementStrategy = "buffer_replacement_strategy";
    static constexpr auto k_LRU_K = "lru_k";

    static constexpr auto k_CheckFinalPlan = "check_final_plan";

    static constexpr auto k_OptimizationEnableHashJoin = "enable_hash_join";
    static constexpr auto k_OptimizationEnableIndexScan = "enable_index_scan";
    static constexpr auto k_OptimizationEnablePredicatePushDown = "enable_predicate_push_down";
    static constexpr auto k_OptimizationDisableOptimization = "no_optimization";

    static constexpr auto k_PrintExecutionStatistics = "print_execution_statistics";

    // this object represents a ConfigValue in the map and stores some meta information
    struct ConfigMapValue
    {
        ConfigValue value;
        bool is_mutable = true;
        bool requires_restart = false;

        explicit operator ConfigValue() const
        {
            return value;
        }

        explicit operator std::size_t() const
        {
            return static_cast<std::size_t>(value);
        }

        explicit operator std::uint32_t() const
        {
            return static_cast<std::uint32_t>(value);
        }

        explicit operator bool() const
        {
            return value != 0u;
        }

        explicit operator BufferReplacementStrategy() const
        {
            return static_cast<BufferReplacementStrategy>(value);
        }

        static constexpr bool immutable = false;
    };

    Config()
    {
        // for transparency, we make compile-time options available in the config map (read only)
        _configuration.insert({k_PageSize, {page_size, ConfigMapValue::immutable}});
        _configuration.insert({k_BPlusTreePageSize, {b_plus_tree_page_size, ConfigMapValue::immutable}});
    }

    ~Config() = default; // TODO: persist changes in config

    /**
     * @brief operator [] Read only access to values of the Configuration!
     *
     * This method is identical to get.
     *
     * @param key
     * @return
     */
    ConfigMapValue operator[](const ConfigKey &key) const
    {
        if (_configuration.find(key) == _configuration.end())
        {
            throw exception::ConfigException(key);
        }
        return _configuration.at(key);
    }

    ConfigMapValue get(const ConfigKey &key) const
    {
        return this->operator[](key);
    }

    /**
     * @brief set sets a configuration value. Can override existing values, if the flag ConfigMapValue.is_mutable is not
     * set!
     * @param key a new or existing key
     * @param value the new value
     * @param is_mutable defaults to true
     * @param requires_restart defaults to false. currently unused.
     * @return the input value, returned from the map
     */
    void set(const ConfigKey &key, ConfigValue value, bool is_mutable = true, bool requires_restart = false)
    {
        if (_configuration.find(key) != _configuration.end())
        {
            // if this key already exists, check if it is read only
            if (!_configuration.at(key).is_mutable)
            {
                throw exception::CanNotModifyAtRuntimeException(key);
            }
        }
        //            // TODO implement: persist config map on shutdown and uncomment this
        //            if (_configuration[key].requires_restart) {
        //                std::cout << "Note: " << "This option requires a restart of the application to take effect!"
        //                << std::endl;
        //            }

        _configuration[key] = ConfigMapValue{value, is_mutable, requires_restart};
    }

    bool contains(const ConfigKey &key) const
    {
        return _configuration.find(key) != _configuration.end();
    }

    explicit operator std::string()
    {
        auto str = std::string{"Current Configuration:\n"};
        for (const auto &[k, value] : _configuration)
        {
            str += "";
            str += std::to_string(value.value);
            //                str +=  ", " + (value.is_mutable ? std::string("r/w") : std::string("r") );

            str += "\t<- " + k + (!value.is_mutable ? " (immutable)" : "") + "\n";
        }
        return str;
    }

  private:
    using ConfigurationMap = std::unordered_map<ConfigKey, ConfigMapValue>;

    // holds the actual configuration values
    ConfigurationMap _configuration;
};
} // namespace beedb
