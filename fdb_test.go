package fdb

import (
	"testing"
)

func TestGenerateOperandEmptyInput(t *testing.T) {
	emptyMap := make(map[string]string)
	result, _ := generateOperand("", []string{}, emptyMap)

	if result != nil {
		t.Errorf("generateOperand(\"\", [\"\"], {}) = %v; expected %v", result, nil)
	}
}
