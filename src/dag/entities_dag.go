package dag

import (
	"dbrestore/utils"
	"fmt"
	"go.uber.org/zap"
	"sort"
)

// log a convenience wrapper to shorten code lines
var log = &utils.Logger

// ChildrenMap the map of child Nodes in DAG, identified by names
type ChildrenMap[T any] map[string][]T

// Node DAG node contains all child Graph Nodes
type Node[T any] struct {
	// Index in the Nodes list, should be larger than 0 after adding to the Graph
	Index int
	// Name in the Graph map, should not be empty after adding to the Graph
	Name string
	// InDegree the count of incoming edges (when this count is 0, it means this node is one of the root Nodes)
	InDegree int
	// SelfCycle indicates whether the DAG node has a self-referential cycle or not.
	SelfCycle bool
	// Children all Nodes pointed to by the parent node
	Children ChildrenMap[T]
	// Error when not empty, this node encountered some problem - something is not supported
	Error string
}

// NewDagNode creates and returns a new node with default values and initialized empty Children map.
func NewDagNode[T any]() Node[T] {
	return Node[T]{
		InDegree:  0,
		SelfCycle: false,
		Children:  make(ChildrenMap[T]),
	}
}

// AddChild adds a child node to the current node with the specified Name and relation. Updates the Children map.
func (n *Node[T]) AddChild(name string, relation T) {
	list, ok := n.Children[name]
	if !ok {
		list = make([]T, 1)
		n.Children[name] = list
	}
	list = append(list, relation)
}

// FKeysGraph the Graph of all tables and FK relations
type FKeysGraph[T any] struct {
	// The list of Nodes stored in the Graph in the order of their insertion, this allows modifying objects directly.
	// The very first element in the list is an empty node, and it is not added to the map by design.
	// This way all subsequent Nodes have indexes larger than 0,
	// and searching in the map returns 0 when nothing is found.
	Nodes []Node[T]
	// the adjacency list of node indexes, stored as a map for efficient lookup
	Graph map[string]int
}

// NewFKeysGraph initializes and returns a new Graph with the specified capacity for its node slice.
func NewFKeysGraph[T any](capacity int) FKeysGraph[T] {
	ret := FKeysGraph[T]{
		Nodes: make([]Node[T], 0, capacity),
		Graph: make(map[string]int),
	}
	// The very first node in the list is an empty node, and it is not added to the map by design.
	// This way all subsequent Nodes have indexes larger than 0,
	// and searching in the map returns 0 when nothing is found.
	ret.Nodes = append(ret.Nodes, NewDagNode[T]())
	return ret
}

// GetNodeCount calculates the number of meaningful Nodes in the Graph, excluding the initial empty node.
func (g *FKeysGraph[T]) GetNodeCount() int {
	ret := len(g.Nodes)
	if ret <= 1 {
		return 0
	}
	return ret - 1
}

// GetGraphSize returns the number of Nodes currently stored in the Graph's adjacency list as a map.
func (g *FKeysGraph[T]) GetGraphSize() int {
	return len(g.Graph)
}

// AddNode adds a new node to the Graph with the specified Name and initializes its properties.
// Returns a pointer to the newly added node or an Error if a node with the given Name already exists.
func (g *FKeysGraph[T]) AddNode(name string) (*Node[T], error) {
	// Ensure the Graph does not already contain a node with the given Name
	index := g.Graph[name]
	if index > 0 {
		return nil, fmt.Errorf("AddNode(): Node with the Name '%s' already exists in the Graph", name)
	}
	g.Nodes = append(g.Nodes, NewDagNode[T]()) // append to the end
	index = len(g.Nodes) - 1                   // use the last Index
	g.Nodes[index].Index = index
	g.Nodes[index].Name = name
	g.Graph[name] = index
	return &g.Nodes[index], nil
}

// GetNode retrieves the node with the specified Name from the Graph.
// Returns a pointer to the node if found, otherwise nil.
func (g *FKeysGraph[T]) GetNode(name string) *Node[T] {
	index := g.Graph[name]
	if index <= 0 {
		return nil
	}
	return &g.Nodes[index]
}

// GetNodeChildren retrieves child Nodes of the node with the specified Name from the Graph.
// Returns a pointer to the node array if found, otherwise nil.
func (g *FKeysGraph[T]) GetNodeChildren(name string) *ChildrenMap[T] {
	index := g.Graph[name]
	if index <= 0 {
		return nil
	}
	return &g.Nodes[index].Children
}

// Helper function for DFS traversal
func (g *FKeysGraph[T]) dfsSort(index int, visited map[string]struct{}, stack []string) []string {
	// Process all Children of the current node
	node := g.Nodes[index]
	// Mark the node as visited
	visited[node.Name] = struct{}{}
	// Create a new slice to hold sorted Children names
	sortedChildren := make([]string, 0, len(node.Children))
	for childName := range node.Children {
		sortedChildren = append(sortedChildren, childName)
	}
	// Sort the slice by Name
	sort.Strings(sortedChildren)
	for _, childName := range sortedChildren {
		childNode := g.GetNode(childName)
		if childNode != nil {
			// If the child is not visited, recursively call dfs
			if _, ok := visited[childNode.Name]; !ok {
				stack = g.dfsSort(childNode.Index, visited, stack)
			}
		} else {
			// if the child does not appear in g.Graph keys, it is still a leaf in the Graph
			if _, ok := visited[childName]; !ok {
				visited[childName] = struct{}{}
				stack = append(stack, childName)
			}
		}
	}
	// Add the node Name to the stack after processing its Children
	return append(stack, node.Name)
}

// TopologicalSort returns the list of names of Graph Nodes in the order that goes from leaves to roots.
// Implemented using topological sorting in a direct acyclic Graph (DAG) using depth-first search (DFS).
func (g *FKeysGraph[T]) TopologicalSort() []string {
	// Create a stack to hold the topological sort order (reversed result, and we want it that way)
	stack := make([]string, 0, len(g.Nodes))
	// Map to track visited Nodes
	visited := make(map[string]struct{}, len(g.Nodes))
	// Extract all Nodes with InDegree == 0, except for the very first which is a fake node
	rootNodes := make([]*Node[T], 0, len(g.Nodes))
	for index, node := range g.Nodes {
		if index > 0 && node.InDegree == 0 { // Skip the fake node and non-root Nodes
			rootNodes = append(rootNodes, &node)
		}
	}
	// Sort the root Nodes by Name
	sort.Slice(rootNodes, func(i, j int) bool {
		return rootNodes[i].Name < rootNodes[j].Name
	})
	// Start DFS from the root Nodes
	for _, node := range rootNodes {
		if _, ok := visited[node.Name]; !ok {
			stack = g.dfsSort(node.Index, visited, stack)
		}
	}
	if len(visited) != len(stack) {
		// I cannot trigger this condition, so cannot cover this code - it just never happens practically.
		// I do not want to remove this test anyway.
		log.Error("TopologicalSort(): FATAL: The number of visited Nodes is not equal to the number of stack elements",
			zap.Int("len(visited)", len(visited)), zap.Int("len(stack)", len(stack)))
		log.Debug("visited: ", zap.Any("visited", visited))
		log.Debug("stack: ", zap.Any("stack", stack))
		return nil
	}
	// Reverse the stack to get the correct topological order - not needed in our case
	//for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
	//	stack[i], stack[j] = stack[j], stack[i]
	//}
	return stack
}

// IsAcyclic Detect if the Graph does not contain cycles, except for self-referencing cycles which are permitted.
// A Graph contains a cycle if you revisit a node currently in the recursion stack (indicating a back edge).
// A self-referencing cycle is when a node referencing to itself - this is okay.
func (g *FKeysGraph[T]) IsAcyclic() bool {
	// Keep track of all visited Nodes using their indexes
	visited := make(map[int]struct{})
	// A slice representing the current recursion stack (Nodes in the current DFS path), using their indexes
	var recStack []int
	ret := true
	for _, index := range g.Graph {
		_, found := visited[index]
		if !found && !g.dfs(index, visited, recStack) {
			ret = false // Cycle detected
		}
	}
	return ret
}

// dfs performs a depth-first search in the provided foreign key relations Graph (fkRelationsMap).
// It ensures there are no cycles in the Graph by keeping track of visited Nodes and a recursion stack.
//
// Parameters:
//   - Graph: The foreign key relations map representing the directed acyclic Graph (DAG).
//   - nodeName: The Name of the current node in the Graph being visited.
//   - visited: A map to keep track of Nodes that have already been visited.
//   - recStack: A slice representing the current recursion stack (Nodes in the current DFS path).
//
// Returns:
//   - bool: Returns true if the Graph is acyclic for the current DFS path; otherwise, false if a cycle is detected.
func (g *FKeysGraph[T]) dfs(index int, visited map[int]struct{}, recStack []int) bool {
	// Mark the current node as visited and add it to the recursion stack.
	visited[index] = struct{}{}
	recStack = append(recStack, index)
	// Iterates over all Children (neighbors) of the current node.
	node := g.Nodes[index]
	ret := true
	for neighborName := range node.Children {
		neighborNode := g.GetNode(neighborName)
		// it is fine if some child Nodes are only leafs in the Graph - skip them
		if neighborNode == nil {
			continue
		}
		// If a neighbor hasn't been visited, performs a recursive DFS call on it.
		if _, visitedNeighbor := visited[neighborNode.Index]; !visitedNeighbor {
			if !g.dfs(neighborNode.Index, visited, recStack) {
				ret = false
			}
			continue
		}
		// If the neighbor is in the recursion stack, a cycle is found
		for _, n := range recStack {
			if n == neighborNode.Index {
				node.Error += "dfs(): Cycle detected: " + neighborNode.Name + " -> " + node.Name + " -> " +
					neighborNode.Name + " -> ... "
				if neighborNode.Index == node.Index {
					// self-cycles are permitted - this is normal in databases
					node.SelfCycle = true
				} else {
					//logger.Error("dfs(): Cycle detected: ", zap.String("node.Name", node.Name),
					//	zap.Int("node.Index", node.Index), zap.Any("recStack", recStack))
					ret = false
				}
			}
		}
	}

	// Remove nodeName from the recursion stack after processing all its neighbors.
	if len(recStack) > 0 && recStack[len(recStack)-1] == index {
		recStack = recStack[:len(recStack)-1]
	} else {
		log.Error("dfs(): FATAL: The node is not found in recStack", zap.String("node.Name", node.Name),
			zap.Int("node.Index", node.Index), zap.Any("recStack", recStack))
		node.Error += "dfs(): FATAL: The node is not found in recStack. "
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

// CalculateInDegree initialize in-degree values for all Nodes to detect root Nodes in the Graph
func (g *FKeysGraph[T]) CalculateInDegree() {
	for _, index := range g.Graph {
		for childTableName := range g.Nodes[index].Children {
			parentNode := g.GetNode(childTableName)
			if parentNode != nil {
				parentNode.InDegree++
			}
		}
	}
}

//// compareTables determines the relative ordering of two tables based on their presence in the Graph and dependencies.
//// Returns -1 if `first` should come before `second`, 1 if `second` should come before `first`, or 0 if they are equal.
//func (g *FKeysGraph[T]) compareTables(first string, second string) int {
//	// If table is not in fkMap, it should appear first
//	firstIndex := g.Graph[first]
//	secondIndex := g.Graph[second]
//
//	// If both are absent in fkMap, sort alphabetically
//	if firstIndex <= 0 && secondIndex <= 0 {
//		return strings.Compare(first, second)
//	}
//
//	// If only one of them is in fkMap, the one that's NOT in fkMap should come first
//	if firstIndex <= 0 {
//		return 1
//	}
//	if secondIndex <= 0 {
//		return -1
//	}
//
//	// If both are in fkMap, check if one depends on the other
//	_, firstDependsOnSecond := g.Nodes[firstIndex].Children[second]
//	_, secondDependsOnFirst := g.Nodes[secondIndex].Children[first]
//
//	// Table i should come after table j if table i depends on table j
//	if firstDependsOnSecond {
//		return 1
//	}
//
//	// Table j should come after table i if table j depends on table i
//	if secondDependsOnFirst {
//		return -1
//	}
//
//	// If neither depends on the other, keep their original order
//	return strings.Compare(first, second)
//}
