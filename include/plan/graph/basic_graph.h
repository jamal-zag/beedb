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

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <set>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace beedb::util
{
/**
 * @brief The Graph class represents a directed graph with "weighted edges".
 *
 * Individual node store objects of type NodeData and have a unique id of type NodeIDType.
 * Edges are directional and carry objects of type EdgeData.
 * There can only be one edge from node A to node B.
 *
 * This class makes extensive use of the subscript operator[]. Edges and nodes can be created and accessed by simply
 * using the operator. For example: `graph[edge] = edge_value`, `NodeDataType value = graph[node]` or
 * `graph[edge].remove()`.
 */
template <typename NodeDataType, typename EdgeDataType, typename NodeIDType> class Graph
{
  public:
    using EdgeID = std::pair<NodeIDType, NodeIDType>; // edge is a pair: <from,to>
    using NodeIDs = std::vector<NodeIDType>;          // a collection of node id's

    Graph() = default;
    Graph(Graph &&) noexcept = default;
    Graph(const Graph &) = delete;
    virtual ~Graph() = default;

  protected:
    struct EdgeIdHash
    {
        std::size_t operator()(const EdgeID &id) const
        {
            return std::hash<NodeIDType>()(std::get<0>(id)) ^ std::hash<NodeIDType>()(std::get<1>(id));
        }
    };

    using NodeData = std::unordered_map<NodeIDType, std::unique_ptr<NodeDataType>>; // holds actual node data
    using EdgeData = std::unordered_map<EdgeID, EdgeDataType, EdgeIdHash>;          // holds actual edge data
    using NodeIDMap = std::unordered_map<NodeIDType, NodeIDs>;                      // used for aux. data structures

    /**
     * @brief The Edges struct Proxy data structure that helps implement
     * the subscript-assignement operators for edges. This is more complicated
     * (compared to node data), because we make use of auxillary data
     * structures for edges (incoming and outgoing edges).
     */
    class Edge
    {
      public:
        Edge(Graph *ref, const EdgeID &edge) : _ref(ref), _edge(edge)
        {
        }
        ~Edge() = default;
        /**
         * @brief operator =
         * This gets called, when a call similar to
         * `graph[{A,B}] = edge_value;` (clone-assignement) is made.
         *
         * This structure is required to keep auxillary structures (incomin- and outgoing nodes) consistent!
         *
         * @param data
         */
        void operator=(const EdgeDataType &data)
        {
            // check graph consistency: we can not add an edge to nodes that dont exist:
            if (_ref->_node_data.find(_edge.first) == _ref->_node_data.end() ||
                _ref->_node_data.find(_edge.second) == _ref->_node_data.end())
            {

                std::cerr << "ERROR: Trying to add edge, but nodes do not exist in graph!"
                          << " Edge is < " << _edge.first << " , " << _edge.second << " >" << std::endl;

                return;
            }

            // the assertions test, if all auxillary structures are properly initialized:
            assert(_ref->_incoming_node_ids.find(_edge.first) != _ref->_incoming_node_ids.end());
            assert(_ref->_incoming_node_ids.find(_edge.second) != _ref->_incoming_node_ids.end());
            assert(_ref->_outgoing_node_ids.find(_edge.first) != _ref->_outgoing_node_ids.end());
            assert(_ref->_outgoing_node_ids.find(_edge.second) != _ref->_outgoing_node_ids.end());

            // create edge:
            _ref->_outgoing_node_ids[_edge.first].push_back(_edge.second);
            _ref->_incoming_node_ids[_edge.second].push_back(_edge.first);
            _ref->_edge_data.insert({_edge, data});
        }
        /**
         * @brief operator EdgeData & conversion operator, that enables the
         * subscript operator to be used as a simple getter for the data behind
         * this proxy object.
         */
        explicit operator const EdgeDataType &() const
        {
            return _ref->_edge_data[_edge];
        }
        /**
         * @brief remove just removes this edge and its data.
         *
         * We also remove it from auxillary data structures ("incoming" and "outgoing" nodes).
         */
        void remove()
        {
            // first, remove edge and its data
            _ref->_edge_data.erase(_edge);
            // then remove id from auxillary edge-structures:
            auto &out_nodes = _ref->_outgoing_node_ids[_edge.first];
            out_nodes.erase(std::remove(out_nodes.begin(), out_nodes.end(), _edge.second), out_nodes.end());

            auto &in_nodes = _ref->_incoming_node_ids[_edge.second];
            in_nodes.erase(std::remove(in_nodes.begin(), in_nodes.end(), _edge.second), in_nodes.end());
        }

      private:
        Graph *_ref;
        const EdgeID &_edge;
    };

    class Node
    {
      public:
        Node(Graph *ref, const NodeIDType &nid) : _ref(ref), _nid(nid)
        {
        }
        /**
         * @brief operator =
         * This gets called, when a call similar to
         * `graph[node_id] = node_data;` is made.
         *
         * This structure is required to keep auxillary structures (incomin- and outgoing nodes) consistent!
         *
         * @param data
         */
        void operator=(std::unique_ptr<NodeDataType> data)
        {
            _ref->insert({_nid, std::move(data)});
        }
        /**
         * @brief operator NodeDataType & conversion operator, that enables the
         * subscript operator to be used as a simple getter for the data behind
         * this proxy object.
         */
        explicit operator std::unique_ptr<NodeDataType> &()
        {
            return _ref->_node_data[_nid];
        }

        explicit operator const NodeDataType &() const
        {
            return **(_ref->_node_data[_nid]);
        }

        [[maybe_unused]] const NodeIDs &incomingNodes() const
        {
            return _ref->_incoming_node_ids[_nid];
        }
        [[maybe_unused]] const NodeIDs &outgoingNodes() const
        {
            return _ref->_outgoing_node_ids[_nid];
        }
        /**
         * @brief remove removes this node and all connected edges.
         */
        void remove()
        {
            // remove node data:
            _ref->_node_data.erase(_nid);

            // remove incoming edges:
            for (auto &other_node_id : _ref->_incoming_node_ids[_nid])
            {
                _ref->_edge_data.erase({other_node_id, _nid}); // actual edge

                // remove edges to this node from _outgoing_node_ids of other_node:
                NodeIDs &out_nodes = _ref->_outgoing_node_ids[other_node_id];
                out_nodes.erase(std::remove(out_nodes.begin(), out_nodes.end(), _nid), out_nodes.end());
            }
            // remove outgoing edges:
            for (auto &other_node_id : _ref->_outgoing_node_ids[_nid])
            {
                _ref->_edge_data.erase({_nid, other_node_id}); // actual edge

                // remove edges to this node from _incoming_node_ids of other_node:
                NodeIDs &in_nodes = _ref->_incoming_node_ids[other_node_id];
                in_nodes.erase(std::remove(in_nodes.begin(), in_nodes.end(), _nid), in_nodes.end());
            }
            // remove auxillary entries for this node:
            _ref->_incoming_node_ids.erase(_nid);
            _ref->_outgoing_node_ids.erase(_nid);
        }

      private:
        Graph *_ref;
        const NodeIDType &_nid;
    };

  public:
    // getter and setter for nodes/their data. can change node's data
    Node operator[](const NodeIDType &nid)
    {
        return Node(this, nid);
    }

    const NodeIDType &insert(
        typename NodeData::value_type &&map_pair) // parameter type is a std::pair<NodeIDType, NodeDataType>
    {
        const NodeIDType &id = _node_data.insert({map_pair.first, std::move(map_pair.second)}).first->first;
        _incoming_node_ids.insert({id, {}});
        _outgoing_node_ids.insert({id, {}});
        return id;
    }

    // simple read-only getter for node-data
    const std::unique_ptr<NodeDataType> &operator[](const NodeIDType &nid) const
    {
        return _node_data.at(nid);
    }

    // getter for mutable edge-data. Access via pair <from,to>
    Edge operator[](const EdgeID &edge)
    {
        return Edge(this, edge);
    }
    // simple read-only getter for edge-data. Access via pair <from,to>
    const EdgeDataType &operator[](const EdgeID &edge_id) const
    {
        return _edge_data[edge_id];
    }
    /**
     * @brief toConsole exhaustive dump of all node- and edge ids to console, without data.
     */
    void toConsole() const
    {
        std::cout << "Nodes:" << std::endl;
        for (const auto &node : _node_data)
        {
            std::cout << "\t{" << node.first << ": " << static_cast<std::string>(*(node.second)) << "}" << std::endl;
        }
        std::cout << "Edges:" << std::endl;
        for (auto const &edge : _edge_data)
        {
            std::cout << "\t<" << edge.first.first << "," << edge.first.second << ">" << std::endl;
        }
    }

    [[maybe_unused]] [[nodiscard]] const NodeIDs &outgoing_nodes(const NodeIDType &nid) const
    {
        return _outgoing_node_ids.at(nid);
    }

    [[nodiscard]] const NodeIDs &incoming_nodes(const NodeIDType &nid) const
    {
        return _incoming_node_ids.at(nid);
    }

    [[nodiscard]] bool empty() const
    {
        return _node_data.empty();
    }

  protected:                      // actual graph data should be available to daughter classes
    NodeData _node_data;          // contains node data. key is a NodeIDType
    EdgeData _edge_data;          // contains edge data. key is a pair <A_ID,B_ID>
    NodeIDMap _outgoing_node_ids; // for efficient connectivity lookup
    NodeIDMap _incoming_node_ids; // for efficient connectivity lookup
};

} // namespace beedb::util
