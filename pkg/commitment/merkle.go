package commitment

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// Domain separation constants
var (
	LeafDomain  = []byte("LEAF:")
	NodeDomain  = []byte("NODE:")
	TableDomain = []byte("TABLE:")
)

// hashWithDomain computes a SHA-256 hash with domain separation
func hashWithDomain(domain []byte, data ...[]byte) [32]byte {
	h := sha256.New()
	
	// Write domain first
	h.Write(domain)
	
	// Write data
	for _, d := range data {
		h.Write(d)
	}
	
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

// BuildMerkleTree builds a Merkle tree from the given leaf data and returns the root hash
func BuildMerkleTree(leafData [][]byte) ([32]byte, error) {
	if len(leafData) == 0 {
		return [32]byte{}, errors.New("empty leaf data")
	}

	// First level: Hash all leaf nodes with leaf domain
	currentLevel := make([][32]byte, len(leafData))
	for i, data := range leafData {
		currentLevel[i] = hashWithDomain(LeafDomain, data)
	}

	// Build the tree bottom-up, level by level
	for len(currentLevel) > 1 {
		nextLevel := make([][32]byte, (len(currentLevel)+1)/2)
		
		// Combine pairs of nodes
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				// Hash pair of nodes
				left := currentLevel[i][:]
				right := currentLevel[i+1][:]
				nextLevel[i/2] = hashWithDomain(NodeDomain, left, right)
			} else {
				// Odd number of nodes, promote the last one
				nextLevel[i/2] = currentLevel[i]
			}
		}
		
		currentLevel = nextLevel
	}
	
	// Return the root
	return currentLevel[0], nil
}

// uint64ToBytes converts a uint64 to a byte slice
func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

// BuildMerkleProof builds a Merkle proof for the leaf at the given index
func BuildMerkleProof(leafData [][]byte, index int) ([][32]byte, error) {
	if index < 0 || index >= len(leafData) {
		return nil, errors.New("index out of bounds")
	}
	
	// First level: Hash all leaf nodes with leaf domain
	currentLevel := make([][32]byte, len(leafData))
	for i, data := range leafData {
		currentLevel[i] = hashWithDomain(LeafDomain, data)
	}
	
	// Initialize proof
	proof := make([][32]byte, 0)
	currentIndex := index
	
	// Build the proof bottom-up, level by level
	for len(currentLevel) > 1 {
		nextLevel := make([][32]byte, (len(currentLevel)+1)/2)
		
		// For each pair of nodes
		for i := 0; i < len(currentLevel); i += 2 {
			// If we have a pair
			if i+1 < len(currentLevel) {
				// If the current node is in our path, add its sibling to the proof
				if i == currentIndex || i+1 == currentIndex {
					siblingIndex := i
					if i == currentIndex {
						siblingIndex = i + 1
					}
					proof = append(proof, currentLevel[siblingIndex])
				}
				
				// Hash pair of nodes
				left := currentLevel[i][:]
				right := currentLevel[i+1][:]
				nextLevel[i/2] = hashWithDomain(NodeDomain, left, right)
			} else {
				// Odd number of nodes, promote the last one
				nextLevel[i/2] = currentLevel[i]
				
				// If this is our node, we don't add anything to the proof since there's no sibling
			}
		}
		
		// Update the index for the next level
		currentIndex = currentIndex / 2
		currentLevel = nextLevel
	}
	
	return proof, nil
}

// VerifyMerkleProof verifies a Merkle proof against the given root and leaf data
func VerifyMerkleProof(root [32]byte, leafData []byte, proof [][32]byte, index int) bool {
	// Start with the leaf hash
	currentHash := hashWithDomain(LeafDomain, leafData)
	
	// Reconstruct the path to the root
	currentIndex := index
	for _, siblingHash := range proof {
		// Determine if the current node is a left or right child
		isLeft := currentIndex%2 == 0
		
		// Combine with sibling in the correct order
		var left, right []byte
		if isLeft {
			left = currentHash[:]
			right = siblingHash[:]
		} else {
			left = siblingHash[:]
			right = currentHash[:]
		}
		
		// Calculate parent hash
		currentHash = hashWithDomain(NodeDomain, left, right)
		
		// Update the index for the next level
		currentIndex = currentIndex / 2
	}
	
	// Check if the calculated root matches the expected root
	return currentHash == root
}