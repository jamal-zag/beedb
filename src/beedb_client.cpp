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

#include <argparse/argparse.hpp>
#include <io/client_console.h>

int main(int arg_count, char **args)
{
    argparse::ArgumentParser argument_parser("beedb_client");
    argument_parser.add_argument("host").help("Name or IP of the beedb server").default_value(std::string("localhost"));
    argument_parser.add_argument("-p", "--port")
        .help("Port of the server")
        .default_value(std::uint16_t(4000))
        .action([](const std::string &value) { return std::uint16_t(std::stoul(value)); });
    try
    {
        argument_parser.parse_args(arg_count, args);
    }
    catch (std::runtime_error &)
    {
        argument_parser.print_help();
        return 1;
    }

    auto server_address = argument_parser.get<std::string>("host");
    auto client = beedb::io::ClientConsole{std::move(server_address), argument_parser.get<std::uint16_t>("-p")};
    client.run();
}