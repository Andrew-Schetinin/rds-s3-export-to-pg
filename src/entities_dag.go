package main

import (
	"fmt"
	"go.uber.org/zap"
	"sort"
	"strings"
)

// ChildrenMap the map of child nodes in DAG, identified by names
type ChildrenMap[T any] map[string][]T

// DagNode DAG node contains all child graph nodes
type DagNode[T any] struct {
	// index in the nodes list, should be larger than 0 after adding to the graph
	index int
	// name in the graph map, should not be empty after adding to the graph
	name string
	// inDegree the count of incoming edges (when this count is 0, it means this node is one of the root nodes)
	inDegree int
	// selfCycle indicates whether the DAG node has a self-referential cycle or not.
	selfCycle bool
	// children all nodes pointed to by the parent node
	children ChildrenMap[T]
	// error when not empty, this node encountered some problem - something is not supported
	error string
}

// NewDagNode creates and returns a new node with default values and initialized empty children map.
func NewDagNode[T any]() DagNode[T] {
	return DagNode[T]{
		inDegree:  0,
		selfCycle: false,
		children:  make(ChildrenMap[T]),
	}
}

// addChild adds a child node to the current node with the specified name and relation. Updates the children map.
func (n *DagNode[T]) addChild(name string, relation T) {
	list, ok := n.children[name]
	if !ok {
		list = make([]T, 1)
		n.children[name] = list
	}
	list = append(list, relation)
}

// FKeysGraph the graph of all tables and FK relations
type FKeysGraph[T any] struct {
	// The list of nodes stored in the graph in the order of their insertion, this allows modifying objects directly.
	// The very first element in the list is an empty node, and it is not added to the map by design.
	// This way all subsequent nodes have indexes larger than 0,
	// and searching in the map returns 0 when nothing is found.
	nodes []DagNode[T]
	// the adjacency list of node indexes, stored as a map for efficient lookup
	graph map[string]int
}

// NewFKeysGraph initializes and returns a new graph with the specified capacity for its node slice.
func NewFKeysGraph[T any](capacity int) FKeysGraph[T] {
	ret := FKeysGraph[T]{
		nodes: make([]DagNode[T], 0, capacity),
		graph: make(map[string]int),
	}
	// The very first node in the list is an empty node, and it is not added to the map by design.
	// This way all subsequent nodes have indexes larger than 0,
	// and searching in the map returns 0 when nothing is found.
	ret.nodes = append(ret.nodes, NewDagNode[T]())
	return ret
}

// getNodeCount calculates the number of meaningful nodes in the graph, excluding the initial empty node.
func (g *FKeysGraph[T]) getNodeCount() int {
	ret := len(g.nodes)
	if ret <= 1 {
		return 0
	}
	return ret - 1
}

// getGraphSize returns the number of nodes currently stored in the graph's adjacency list as a map.
func (g *FKeysGraph[T]) getGraphSize() int {
	return len(g.graph)
}

// addNode adds a new node to the graph with the specified name and initializes its properties.
// Returns a pointer to the newly added node or an error if a node with the given name already exists.
func (g *FKeysGraph[T]) addNode(name string) (*DagNode[T], error) {
	// Ensure the graph does not already contain a node with the given name
	index := g.graph[name]
	if index > 0 {
		return nil, fmt.Errorf("addNode(): Node with the name '%s' already exists in the graph", name)
	}
	g.nodes = append(g.nodes, NewDagNode[T]()) // append to the end
	index = len(g.nodes) - 1                   // use the last index
	g.nodes[index].index = index
	g.nodes[index].name = name
	g.graph[name] = index
	return &g.nodes[index], nil
}

// getNode retrieves the node with the specified name from the graph.
// Returns the pointer to the node and true if found, otherwise nil and false.
func (g *FKeysGraph[T]) getNode(name string) *DagNode[T] {
	index := g.graph[name]
	if index <= 0 {
		return nil
	}
	return &g.nodes[index]
}

// Helper function for DFS traversal
func (g *FKeysGraph[T]) dfsSort(index int, visited map[string]struct{}, stack []string) []string {
	// Process all children of the current node
	node := g.nodes[index]
	// Mark the node as visited
	visited[node.name] = struct{}{}
	// Create a new slice to hold sorted children names
	sortedChildren := make([]string, 0, len(node.children))
	for childName := range node.children {
		sortedChildren = append(sortedChildren, childName)
	}
	// Sort the slice by name
	sort.Strings(sortedChildren)
	for _, childName := range sortedChildren {
		childNode := g.getNode(childName)
		if childNode != nil {
			// If the child is not visited, recursively call dfs
			if _, ok := visited[childNode.name]; !ok {
				stack = g.dfsSort(childNode.index, visited, stack)
			}
		} else {
			// if the child does not appear in g.graph keys, it is still a leaf in the graph
			if _, ok := visited[childName]; !ok {
				visited[childName] = struct{}{}
				stack = append(stack, childName)
			}
		}
	}
	// Add the node name to the stack after processing its children
	return append(stack, node.name)
}

// topologicalSort returns the list of names of graph nodes in the order that goes from leaves to roots.
// Implemented using topological sorting in a direct acyclic graph (DAG) using depth-first search (DFS).
func (g *FKeysGraph[T]) topologicalSort() []string {
	// Create a stack to hold the topological sort order (reversed result, and we want it that way)
	stack := make([]string, 0, len(g.nodes))
	// Map to track visited nodes
	visited := make(map[string]struct{}, len(g.nodes))
	// Extract all nodes with inDegree == 0, except for the very first which is a fake node
	rootNodes := make([]*DagNode[T], 0, len(g.nodes))
	for index, node := range g.nodes {
		if index > 0 && node.inDegree == 0 { // Skip the fake node and non-root nodes
			rootNodes = append(rootNodes, &node)
		}
	}
	// Sort the root nodes by name
	sort.Slice(rootNodes, func(i, j int) bool {
		return rootNodes[i].name < rootNodes[j].name
	})
	// Start DFS from the root nodes
	for _, node := range rootNodes {
		if _, ok := visited[node.name]; !ok {
			stack = g.dfsSort(node.index, visited, stack)
		}
	}
	if len(visited) != len(stack) {
		logger.Error("topologicalSort(): FATAL: The number of visited nodes is not equal to the number of stack elements",
			zap.Int("len(visited)", len(visited)), zap.Int("len(stack)", len(stack)))
		logger.Debug("visited: ", zap.Any("visited", visited))
		logger.Debug("stack: ", zap.Any("stack", stack))
		return nil
	}
	// Reverse the stack to get the correct topological order - not needed in our case
	//for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
	//	stack[i], stack[j] = stack[j], stack[i]
	//}
	return stack
}

// Detect if the graph does not contain cycles, except for self-referencing cycles which are permitted.
// A graph contains a cycle if you revisit a node currently in the recursion stack (indicating a back edge).
// A self-referencing cycle is when a node referencing to itself - this is okay.
func (g *FKeysGraph[T]) isAcyclic() bool {
	// Keep track of all visited nodes using their indexes
	visited := make(map[int]struct{})
	// A slice representing the current recursion stack (nodes in the current DFS path), using their indexes
	var recStack []int
	ret := true
	for _, index := range g.graph {
		_, found := visited[index]
		if !found && !g.dfs(index, visited, recStack) {
			ret = false // Cycle detected
		}
	}
	return ret
}

// dfs performs a depth-first search in the provided foreign key relations graph (fkRelationsMap).
// It ensures there are no cycles in the graph by keeping track of visited nodes and a recursion stack.
//
// Parameters:
//   - graph: The foreign key relations map representing the directed acyclic graph (DAG).
//   - nodeName: The name of the current node in the graph being visited.
//   - visited: A map to keep track of nodes that have already been visited.
//   - recStack: A slice representing the current recursion stack (nodes in the current DFS path).
//
// Returns:
//   - bool: Returns true if the graph is acyclic for the current DFS path; otherwise, false if a cycle is detected.
func (g *FKeysGraph[T]) dfs(index int, visited map[int]struct{}, recStack []int) bool {
	// Mark the current node as visited and add it to the recursion stack.
	visited[index] = struct{}{}
	recStack = append(recStack, index)
	// Iterates over all children (neighbors) of the current node.
	node := g.nodes[index]
	ret := true
	for neighborName := range node.children {
		neighborNode := g.getNode(neighborName)
		// it is fine if some child nodes are only leafs in the graph - skip them
		if neighborNode == nil {
			continue
		}
		// If a neighbor hasn't been visited, performs a recursive DFS call on it.
		if _, visitedNeighbor := visited[neighborNode.index]; !visitedNeighbor {
			if !g.dfs(neighborNode.index, visited, recStack) {
				ret = false
			}
			continue
		}
		// If the neighbor is in the recursion stack, a cycle is found
		for _, n := range recStack {
			if n == neighborNode.index {
				node.error += "dfs(): Cycle detected: " + neighborNode.name + " -> " + node.name + " -> " +
					neighborNode.name + " -> ... "
				if neighborNode.index == node.index {
					// self-cycles are permitted - this is normal in databases
					node.selfCycle = true
				} else {
					//logger.Error("dfs(): Cycle detected: ", zap.String("node.name", node.name),
					//	zap.Int("node.index", node.index), zap.Any("recStack", recStack))
					ret = false
				}
			}
		}
	}
	// Remove nodeName from the recursion stack after processing all its neighbors.
	if len(recStack) > 0 && recStack[len(recStack)-1] == index {
		recStack = recStack[:len(recStack)-1]
	} else {
		logger.Error("dfs(): FATAL: The node is not found in recStack", zap.String("node.name", node.name),
			zap.Int("node.index", node.index), zap.Any("recStack", recStack))
		node.error += "dfs(): FATAL: The node is not found in recStack. "
		ret = false // we definitely have a problem even if it is not a cycle
	}
	//for i, n := range recStack {
	//	if n == nodeName {
	//		recStack = append(recStack[:i], recStack[i+1:]...)
	//		break
	//	}
	//}
	// If no cycles are found, returns true.
	return ret
}

// compareTables determines the relative ordering of two tables based on their presence in the graph and dependencies.
// Returns -1 if `first` should come before `second`, 1 if `second` should come before `first`, or 0 if they are equal.
func (g *FKeysGraph[T]) compareTables(first string, second string) int {
	// If table is not in fkMap, it should appear first
	firstIndex := g.graph[first]
	secondIndex := g.graph[second]

	// If both are absent in fkMap, sort alphabetically
	if firstIndex <= 0 && secondIndex <= 0 {
		return strings.Compare(first, second)
	}

	// If only one of them is in fkMap, the one that's NOT in fkMap should come first
	if firstIndex <= 0 {
		return 1
	}
	if secondIndex <= 0 {
		return -1
	}

	// If both are in fkMap, check if one depends on the other
	_, firstDependsOnSecond := g.nodes[firstIndex].children[second]
	_, secondDependsOnFirst := g.nodes[secondIndex].children[first]

	// Table i should come after table j if table i depends on table j
	if firstDependsOnSecond {
		return 1
	}

	// Table j should come after table i if table j depends on table i
	if secondDependsOnFirst {
		return -1
	}

	// If neither depends on the other, keep their original order
	return strings.Compare(first, second)
}

// calculateInDegree initialize in-degree values for all nodes to detect root nodes in the graph
func (g *FKeysGraph[T]) calculateInDegree() {
	for _, index := range g.graph {
		for childTableName := range g.nodes[index].children {
			parentNode := g.getNode(childTableName)
			if parentNode != nil {
				parentNode.inDegree++
			}
		}
	}
}
