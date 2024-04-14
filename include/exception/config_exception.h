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
#include "exception.h"
#include <string>

namespace beedb::exception
{
class ConfigException : public DatabaseException
{
  public:
    explicit ConfigException(const std::string &message) : DatabaseException(DatabaseException::Configuration, message)
    {
    }
    ~ConfigException() override = default;
};

class KeyNotFoundException final : public ConfigException
{
  public:
    explicit KeyNotFoundException(const std::string &key) : ConfigException("Option " + key + " not found.")
    {
    }

    ~KeyNotFoundException() override = default;
};

class CanNotModifyAtRuntimeException final : public ConfigException
{
  public:
    explicit CanNotModifyAtRuntimeException(const std::string &key)
        : ConfigException("Option " + key + " can not be changed at runtime.")
    {
    }

    ~CanNotModifyAtRuntimeException() override = default;
};

} // namespace beedb::exception