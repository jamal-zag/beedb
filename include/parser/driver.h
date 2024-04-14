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

#include "location.hh"
#include "node.h"
#include <iostream>
#include <memory>
#include <vector>

namespace beedb::parser
{

class Parser;

class Driver
{
  public:
    Driver() noexcept = default;
    ~Driver() noexcept = default;

    int parse(std::istream &&in);

    [[nodiscard]] const std::unique_ptr<NodeInterface> &ast() const
    {
        return _root;
    }
    [[nodiscard]] std::unique_ptr<NodeInterface> &ast()
    {
        return _root;
    }

    void ast(std::unique_ptr<NodeInterface> &&root)
    {
        _root = std::move(root);
    }
    friend class Parser;
    friend class Scanner;

  private:
    std::unique_ptr<NodeInterface> _root;
};
} // namespace beedb::parser
