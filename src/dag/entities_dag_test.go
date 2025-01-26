package dag

import (
	"testing"
)

type TestMap map[string][]string

// newGraph creates and initializes a new Graph from the provided TestMap.
// It adds Nodes and their relationships based on the input, calculating in-degree values for all Nodes.
// Returns a pointer to the constructed Graph or nil if an Error occurs during node creation.
func newGraph(nodes TestMap) *FKeysGraph[string] {
	ret := NewFKeysGraph[string](10)
	for key, children := range nodes {
		node, err := ret.AddNode(key)
		if err != nil {
			return nil
		}
		for _, child := range children {
			node.AddChild(child, "")
		}
	}
	ret.CalculateInDegree()
	return &ret
}

func TestCount(t *testing.T) {
	t.Run("Test count", func(t *testing.T) {
		graph := *newGraph(TestMap{
			"A": {"B"},
			"B": {"C"},
			"C": {},
		})
		if result := graph.GetNodeCount(); result != 3 {
			t.Errorf("GetNodeCount(%v) = %v; want %v", graph, result, 3)
		}
		if result := graph.GetGraphSize(); result != 3 {
			t.Errorf("GetGraphSize(%v) = %v; want %v", graph, result, 3)
		}
	})
}

func TestAddNodeError(t *testing.T) {
	t.Run("Test AddNode Error", func(t *testing.T) {
		graph := *newGraph(TestMap{
			"A": {"B"},
			"B": {"C"},
			"C": {},
		})
		_, err := graph.AddNode("A")
		if err == nil {
			t.Errorf("AddNode() was supposed to return an Error")
		}
	})
}

func TestIsAcyclic(t *testing.T) {
	tests := []struct {
		name           string
		graph          FKeysGraph[string]
		expectedResult bool
	}{
		{
			name: "Graph with no cycles",
			graph: *newGraph(TestMap{
				"A": {"B"},
				"B": {"C"},
				"C": {},
			}),
			expectedResult: true,
		},
		{
			name: "Graph with a cycle",
			graph: *newGraph(TestMap{
				"A": {"B"},
				"B": {"C"},
				"C": {"A"},
			}),
			expectedResult: false,
		},
		{
			name:           "Empty Graph",
			graph:          NewFKeysGraph[string](1),
			expectedResult: true, // An empty Graph is acyclic
		},
		{
			name: "Single node with no edges",
			graph: *newGraph(TestMap{
				"A": {},
			}),
			expectedResult: true,
		},
		{
			name: "Disconnected Graph with multiple roots",
			graph: *newGraph(TestMap{
				"A": {"B"},
				"B": {},
				"C": {"D"},
				"D": {},
			}),
			expectedResult: true, // No cycles present
		},
		{
			name: "Self-referencing node is not a cycle",
			graph: *newGraph(TestMap{
				"A": {"B", "C"},
				"B": {"C"},
				"C": {"C"},
			}),
			expectedResult: true, // No cycles present
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.graph.IsAcyclic()
			if result != tt.expectedResult {
				t.Errorf("IsAcyclic(%v) = %v; want %v", tt.graph, result, tt.expectedResult)
			}
		})
	}
}

func TestTopologicalSort(t *testing.T) {
	tests := []struct {
		name           string
		graph          FKeysGraph[string]
		expectedResult []string
	}{
		{
			name: "Graph with no cycles",
			graph: *newGraph(TestMap{
				"A": {"B"},
				"B": {"C"},
				"C": {},
			}),
			expectedResult: []string{"C", "B", "A"},
		},
		{
			name:           "Empty Graph",
			graph:          NewFKeysGraph[string](1),
			expectedResult: []string{}, // An empty Graph is acyclic
		},
		{
			name: "Single node with no edges",
			graph: *newGraph(TestMap{
				"A": {},
			}),
			expectedResult: []string{"A"},
		},
		{
			name: "Disconnected Graph with multiple roots",
			graph: *newGraph(TestMap{
				"A": {"B"},
				"B": {},
				"C": {"D"},
				"D": {},
				"E": {},
			}),
			expectedResult: []string{"B", "A", "D", "C", "E"}, // No cycles present
		},
		{
			name: "Disconnected Graph with multiple roots and shared Children",
			graph: *newGraph(TestMap{
				"E": {"G", "D", "B"},
				"A": {"B"},
				"F": {},
				"B": {},
				"C": {"D", "G"}, // G is a leaf node appearing twice - it caused a bug
				"D": {"D"},
			}),
			expectedResult: []string{"B", "A", "D", "G", "C", "E", "F"}, // No cycles present
		},
		{
			name: "Self-referencing node is not a cycle",
			graph: *newGraph(TestMap{
				"A": {"B", "C"},
				"B": {"C"},
				"C": {"C"},
			}),
			expectedResult: []string{"C", "B", "A"}, // No cycles present
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.graph.TopologicalSort()
			if !equalArrays(result, tt.expectedResult) {
				t.Errorf("TopologicalSort(%v) = %v; want %v", tt.graph, result, tt.expectedResult)
			}
		})
	}
}

// equalArrays compares two string slices for equality and returns true if they have the same length and elements.
func equalArrays(result1 []string, result2 []string) bool {
	if len(result1) != len(result2) {
		return false
	}
	for i := 0; i < len(result1); i++ {
		if result1[i] != result2[i] {
			return false
		}
	}
	return true
}
