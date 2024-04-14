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

#include <netdb.h>
#include <netinet/in.h>
#include <network/client.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace beedb::network;

Client::Client(std::string &&server, std::uint16_t port) : _server(std::move(server)), _port(port), _socket(-1)
{
}

bool Client::connect()
{
    this->_socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (this->_socket < 0)
    {
        return false;
    }

    struct addrinfo hints
    {
    }, *resource = nullptr;
    hints.ai_flags = AI_CANONNAME;
    hints.ai_family = AF_INET;
    hints.ai_socktype = 0;
    hints.ai_protocol = 0;

    const auto port = std::to_string(static_cast<std::int16_t>(this->_port));
    int result = ::getaddrinfo(this->_server.data(), port.c_str(), &hints, &resource);
    if (result == 0)
    {
        auto *current = resource;
        while (current != nullptr)
        {
            if (::connect(this->_socket, current->ai_addr, current->ai_addrlen) == 0)
            {
                ::freeaddrinfo(resource);
                return true;
            }
            current = current->ai_next;
        }
    }

    return false;
}

void Client::disconnect() const
{
    ::close(this->_socket);
}

std::string Client::send(const std::string &message)
{
    const auto w = ::write(this->_socket, message.data(), message.size());
    if (w < 0)
    {
        return std::string("Error on sending message.");
    }

    // Read header
    auto header = std::uint64_t(0);
    this->read_into_buffer(sizeof(header), static_cast<void *>(&header));

    // Read data
    auto message_buffer = std::string(header, '\0');
    this->read_into_buffer(header, message_buffer.data());

    return message_buffer;
}

std::uint64_t Client::read_into_buffer(std::uint64_t length, void *buffer) const
{
    auto bytes_read = 0u;
    while (bytes_read < length)
    {
        const auto read =
            ::read(this->_socket, reinterpret_cast<void *>(reinterpret_cast<std::uintptr_t>(buffer) + bytes_read),
                   length - bytes_read);
        if (read < 1)
        {
            return bytes_read;
        }

        bytes_read += read;
    }

    return bytes_read;
}