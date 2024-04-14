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

#include <io/file_executor.h>
#include <regex>
#include <sstream>

using namespace beedb::io;

void FileExecutor::execute(const std::string &file_name)
{
    auto file = std::ifstream{file_name};
    if (file.is_open() == false)
    {
        std::cout << "[Error] File '" << file_name << "' not found." << std::endl;
        return;
    }

    const auto file_ending = file_name.substr(file_name.size() - 4U);
    if (file_ending == ".sql")
    {
        this->execute_sql_file(std::move(file));
    }
    else if (file_ending == ".tbl")
    {
        this->execute_tbl_file(file_name, std::move(file));
    }
    else
    {
        std::cout << "[Error] File '" << file_name << "' is not supported." << std::endl;
    }
}

void FileExecutor::execute_sql_file(std::ifstream &&file)
{
    std::stringstream statement_stream;
    std::string line;
    std::vector<std::string> statements;

    std::regex comment_regex(".*#|.*--.*");
    std::regex long_comment_start_regex(".*\\/\\*");
    std::regex long_comment_end_regex(".*\\*\\/");
    std::regex statement_end_regex(".*;");
    std::smatch match;
    bool is_within_comment = false;

    while (std::getline(file, line))
    {
        if (std::regex_match(line, match, long_comment_end_regex))
        {
            is_within_comment = false;
            continue;
        }

        if (is_within_comment || std::regex_match(line, match, comment_regex))
        {
            continue;
        }

        if (std::regex_match(line, match, long_comment_start_regex))
        {
            is_within_comment = true;
            continue;
        }

        statement_stream << " " << line << std::flush;
        if (std::regex_match(line, match, statement_end_regex))
        {
            statements.push_back(statement_stream.str());
            statement_stream.str("");
            statement_stream.clear();
        }
    }

    execute_statements(std::move(statements));
}

void FileExecutor::execute_tbl_file(const std::string &file_name, std::ifstream &&file)
{
    auto table_name = file_name.substr(0, file_name.find_last_of('.'));
    const auto last_path = file_name.find_last_of('/');
    if (last_path != std::string::npos)
    {
        table_name = table_name.substr(last_path + 1U);
    }

    if (this->_database.table_exists(table_name) == false)
    {
        std::cerr << "Table " << table_name << " does not exist." << std::endl;
        return;
    }

    const auto &schema = this->_database.table(table_name)->schema();
    std::stringstream insert_stream;
    insert_stream << "insert into " << table_name << " (";
    for (auto i = 0U; i < schema.size(); ++i)
    {
        if (i > 0U)
        {
            insert_stream << ",";
        }
        insert_stream << schema.terms()[i].get<expression::Attribute>().column_name();
    }
    insert_stream << ") values ";
    const auto insert_template = insert_stream.str();

    std::string line;
    std::vector<std::string> statements;

    while (std::getline(file, line))
    {
        std::vector<std::string> values;
        values.reserve(schema.size());
        std::stringstream value_stream{line};
        std::string value;
        while (std::getline(value_stream, value, '|'))
        {
            values.push_back(value);
        }

        if (values.size() >= schema.size())
        {
            std::stringstream statement_stream;
            statement_stream << insert_template << " (";

            for (auto i = 0U; i < schema.size(); ++i)
            {
                if (i > 0U)
                {
                    statement_stream << ",";
                }

                const auto as_string = schema[i].type() == table::Type::CHAR || schema[i].type() == table::Type::DATE;
                if (as_string)
                {
                    statement_stream << "'";
                }
                statement_stream << values[i];
                if (as_string)
                {
                    statement_stream << "'";
                }
            }

            statement_stream << ");";
            statements.emplace_back(statement_stream.str());
        }
    }

    execute_statements(std::move(statements));
}

void FileExecutor::execute_statements(std::vector<std::string> &&statements)
{
    const auto size = statements.size();
    auto current = std::size_t{0};

    for (const auto &statement : statements)
    {
        const auto c = ++current;
        const auto result = Executor::execute(Query{statement});
        if (result.is_successful() == false)
        {
            std::cerr << "Error on executing statement #" << c << ": " << result.error() << std::endl;
            break;
        }

        std::cout << "\rExecuted " << c << " / " << size << " (" << std::uint16_t(100 / float(size) * float(c)) << "%)"
                  << std::flush;
    }

    if (this->_database.transaction_manager().commit(*this->_transaction))
    {
        std::cout << "\r"
                  << "\033[0;32m"
                  << "Executed " << size << " statements"
                  << " \033[0m";
        std::cout << std::string(20, ' ') << std::endl;
    }
    else
    {
        std::cerr << "Could not commit." << std::endl;
    }
}
