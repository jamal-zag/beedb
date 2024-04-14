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

#include <array>
#include <config.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace beedb::network
{
class Server;
class ClientHandler
{
  public:
    ClientHandler() noexcept = default;
    virtual ~ClientHandler() noexcept = default;
    virtual std::optional<std::string> handle_message(const std::uint32_t client_id, const std::string &message) = 0;
    virtual void on_client_connected(const std::uint32_t id) = 0;
    virtual void on_client_disconnected(const std::uint32_t id) = 0;
    virtual void server(Server *server)
    {
        _server = server;
    }

  protected:
    [[nodiscard]] Server *server() const
    {
        return _server;
    }

  private:
    Server *_server = nullptr;
};

class Server
{
  public:
    Server(ClientHandler &message_handler, std::uint16_t port) noexcept;
    ~Server() noexcept = default;

    [[nodiscard]] std::uint16_t port() const noexcept
    {
        return _port;
    }
    [[nodiscard]] bool is_running() const noexcept
    {
        return _is_running;
    }
    void stop() noexcept
    {
        _is_running = false;
    }
    void send(std::uint32_t client_id, std::string &&message);
    bool listen();

  private:
    const std::uint16_t _port;
    std::int32_t _socket;
    std::array<std::uint32_t, Config::max_clients> _client_sockets;
    std::array<char, 512> _buffer;
    ClientHandler &_handler;

    alignas(64) bool _is_running = true;

    std::uint16_t add_client(std::int32_t client_socket);
};
} // namespace beedb::network