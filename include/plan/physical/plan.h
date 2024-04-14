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
#include <database.h>
#include <execution/operator_interface.h>
#include <functional>
#include <io/execution_callback.h>
#include <optional>
#include <table/schema.h>
#include <table/tuple.h>
#include <vector>

namespace beedb::plan::physical
{
/**
 * The plan grants access to the physical operator chain.
 */
class Plan
{
  public:
    Plan(Database &database, std::unique_ptr<execution::OperatorInterface> &&root)
        : _database(database), _root(std::move(root))
    {
    }

    ~Plan() = default;

    /**
     * Executes the physical operators.
     *
     * @param schema_callback Will be called once, when the output schema is not empty.
     * @param row_callback Will be called for every output tuple.
     */
    std::uint64_t execute(io::ExecutionCallback &execution_callback);

  protected:
    Database &_database;
    std::unique_ptr<execution::OperatorInterface> _root;
};
} // namespace beedb::plan::physical
