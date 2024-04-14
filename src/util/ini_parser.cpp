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

#include <fstream>
#include <functional>
#include <regex>
#include <util/ini_parser.h>

using namespace beedb::util;

IniParser::IniParser(const std::string &file_name)
{
    this->parse(file_name);
}

void IniParser::parse(const std::string &file_name)
{
    std::regex section_regex(R"(^\[([\w\s\-\.]+)\]\s*$)");
    std::regex item_regex(R"(^([\w\s\-_\.]+)\s*=\s*([\w\-_\.]+)\s*\;?.*$)");
    std::smatch match;

    std::string line;
    std::ifstream file_stream(file_name);
    std::string current_section{};

    while (std::getline(file_stream, line))
    {
        if (line.empty() == false)
        {
            if (std::regex_match(line, match, section_regex))
            {
                current_section = match[1].str();
            }
            else if (std::regex_match(line, match, item_regex))
            {
                auto key = match[1].str();
                const auto value = match[2].str();

                const auto not_eq_space = std::bind(std::not_equal_to<char>(), std::placeholders::_1, ' ');
                // Remove leading and trailing spaces from key
                key.erase(key.begin(), std::find_if(key.begin(), key.end(), not_eq_space));
                key.erase(std::find_if(key.rbegin(), key.rend(), not_eq_space).base(), key.end());

                auto config_key = key_t{std::make_pair(current_section, std::move(key))};
                this->_configurations.insert(std::make_pair(config_key, std::move(value)));
            }
        }
    }
}
