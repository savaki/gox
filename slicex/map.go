// Package slicex provides extensions for working with slices in Go.
package slicex

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
)

// Map applies a transformation function to each element of a slice,
// returning a new slice with the transformed elements of type U.
func Map[T, U any](slice []T, fn func(T) U) []U {
	if slice == nil {
		return nil
	}

	result := make([]U, len(slice))
	for i, item := range slice {
		result[i] = fn(item)
	}
	return result
}

// ConcurrentMapper provides concurrent mapping functionality with configurable options.
type ConcurrentMapper[T, U any] struct {
	fn            func(context.Context, T) (U, error)
	concurrency   int
	collectErrors bool
}

// MapConcurrent creates a new ConcurrentMapper with the given transformation function.
func MapConcurrent[T, U any](fn func(context.Context, T) (U, error)) *ConcurrentMapper[T, U] {
	return &ConcurrentMapper[T, U]{
		fn:          fn,
		concurrency: 12,
	}
}

// Concurrency sets the maximum number of concurrent goroutines.
func (cm *ConcurrentMapper[T, U]) Concurrency(n int) *ConcurrentMapper[T, U] {
	cm.concurrency = n
	return cm
}

// CollectErrors enables collecting all errors instead of failing fast.
func (cm *ConcurrentMapper[T, U]) CollectErrors() *ConcurrentMapper[T, U] {
	cm.collectErrors = true
	return cm
}

// workItem represents a unit of work for the worker pool.
type workItem[T, U any] struct {
	index int
	value T
}

// DoValues executes the concurrent mapping on the provided inputs.
func (cm *ConcurrentMapper[T, U]) DoValues(ctx context.Context, inputs ...T) ([]U, error) {
	if len(inputs) == 0 {
		return []U{}, nil
	}

	results := make([]U, len(inputs))
	errs := make([]error, len(inputs))

	workChan := make(chan workItem[T, U], len(inputs))
	var wg sync.WaitGroup

	// Create subcontext for fail-fast cancellation
	child, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start workers
	for i := 0; i < cm.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workChan {
				select {
				case <-child.Done():
					return
				default:
				}

				result, err := cm.fn(child, work.value)
				if err != nil {
					errs[work.index] = err
					if !cm.collectErrors {
						cancel() // Cancel all other workers
						return
					}
				} else {
					results[work.index] = result
				}
			}
		}()
	}

	// Send work to workers
	go func() {
		defer close(workChan)
		for i, input := range inputs {
			select {
			case <-child.Done():
				return
			case workChan <- workItem[T, U]{index: i, value: input}:
			}
		}
	}()

	wg.Wait()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if cm.collectErrors {
		return results, errors.Join(errs...)
	}

	// Return first error in fail-fast mode
	for _, err := range errs {
		if err != nil {
			return results, err
		}
	}

	return results, nil
}

// Filter returns a new slice containing only the elements from the input slice
// that satisfy all the provided predicate functions. All predicates must return
// true for an element to be included in the result (AND logic).
func Filter[T any](slice []T, predicates ...func(T) bool) []T {
	if slice == nil {
		return nil
	}
	
	if len(predicates) == 0 {
		// No predicates means all elements pass
		result := make([]T, len(slice))
		copy(result, slice)
		return result
	}

	var result []T
	for _, item := range slice {
		passes := true
		for _, predicate := range predicates {
			if !predicate(item) {
				passes = false
				break
			}
		}
		if passes {
			result = append(result, item)
		}
	}

	return result
}

// FilterNonzero returns a new slice containing only the elements from the input slice
// that are not the zero value for type T.
func FilterNonzero[T comparable](slice []T) []T {
	if slice == nil {
		return nil
	}

	var zero T
	var result []T
	for _, item := range slice {
		if item != zero {
			result = append(result, item)
		}
	}

	return result
}

// Take returns a new slice containing up to the first n elements from the input slice.
// If n is greater than the slice length, returns all elements.
// If n is negative or zero, returns an empty slice.
// If the input slice is nil, returns nil.
func Take[T any](slice []T, n int) []T {
	if slice == nil {
		return nil
	}
	
	if n <= 0 {
		return []T{}
	}
	
	if n >= len(slice) {
		result := make([]T, len(slice))
		copy(result, slice)
		return result
	}
	
	result := make([]T, n)
	copy(result, slice[:n])
	return result
}

// Skip returns a new slice with the first n elements removed.
// If n is greater than the slice length, returns an empty slice.
// If n is negative or zero, returns a copy of the entire slice.
// If the input slice is nil, returns nil.
func Skip[T any](slice []T, n int) []T {
	if slice == nil {
		return nil
	}
	
	if n <= 0 {
		result := make([]T, len(slice))
		copy(result, slice)
		return result
	}
	
	if n >= len(slice) {
		return []T{}
	}
	
	result := make([]T, len(slice)-n)
	copy(result, slice[n:])
	return result
}

// Reduce applies a function cumulatively to reduce slice to single value.
// If the slice is nil or empty, returns the initial value.
func Reduce[T, U any](slice []T, initial U, fn func(U, T) U) U {
	if slice == nil {
		return initial
	}
	
	result := initial
	for _, item := range slice {
		result = fn(result, item)
	}
	return result
}

// Find returns the first element and its index that matches the predicate.
// Returns zero value, -1, false if not found or slice is nil.
func Find[T any](slice []T, predicate func(T) bool) (T, int, bool) {
	var zero T
	if slice == nil {
		return zero, -1, false
	}
	
	for i, item := range slice {
		if predicate(item) {
			return item, i, true
		}
	}
	return zero, -1, false
}

// Any returns true if any element matches the predicate.
// Returns false for nil or empty slice.
func Any[T any](slice []T, predicate func(T) bool) bool {
	if slice == nil {
		return false
	}
	
	for _, item := range slice {
		if predicate(item) {
			return true
		}
	}
	return false
}

// All returns true if all elements match the predicate.
// Returns true for nil or empty slice (vacuous truth).
func All[T any](slice []T, predicate func(T) bool) bool {
	if slice == nil {
		return true
	}
	
	for _, item := range slice {
		if !predicate(item) {
			return false
		}
	}
	return true
}

// Sum returns the sum of all numeric elements in the slice.
// Returns zero for nil or empty slice.
func Sum[T ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64](slice []T) T {
	var result T
	if slice == nil {
		return result
	}
	
	for _, item := range slice {
		result += item
	}
	return result
}

// Reverse returns a new slice with elements in reverse order.
// If the input slice is nil, returns nil.
func Reverse[T any](slice []T) []T {
	if slice == nil {
		return nil
	}
	
	result := make([]T, len(slice))
	for i, item := range slice {
		result[len(slice)-1-i] = item
	}
	return result
}

// Unique returns a new slice with duplicate elements removed.
// Order of first occurrence is preserved.
// If the input slice is nil, returns nil.
func Unique[T comparable](slice []T) []T {
	if slice == nil {
		return nil
	}
	
	seen := make(map[T]bool)
	var result []T
	
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	
	return result
}

// Partition splits slice into two based on predicate.
// Returns (matching, non-matching) elements.
// If the input slice is nil, returns (nil, nil).
func Partition[T any](slice []T, predicate func(T) bool) ([]T, []T) {
	if slice == nil {
		return nil, nil
	}
	
	var matching, nonMatching []T
	
	for _, item := range slice {
		if predicate(item) {
			matching = append(matching, item)
		} else {
			nonMatching = append(nonMatching, item)
		}
	}
	
	return matching, nonMatching
}

// GroupBy groups elements by a key function, returns map of key to slice.
// If the input slice is nil, returns nil.
func GroupBy[T any, K comparable](slice []T, keyFn func(T) K) map[K][]T {
	if slice == nil {
		return nil
	}
	
	result := make(map[K][]T)
	
	for _, item := range slice {
		key := keyFn(item)
		result[key] = append(result[key], item)
	}
	
	return result
}

// Chunk splits slice into chunks of specified size.
// Last chunk may be smaller than size.
// If size <= 0, returns empty slice of slices.
// If the input slice is nil, returns nil.
func Chunk[T any](slice []T, size int) [][]T {
	if slice == nil {
		return nil
	}
	
	if size <= 0 {
		return [][]T{}
	}
	
	var result [][]T
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}
		chunk := make([]T, end-i)
		copy(chunk, slice[i:end])
		result = append(result, chunk)
	}
	
	return result
}

// Union returns unique elements from all input slices.
// Order of first occurrence across all slices is preserved.
// If all input slices are nil, returns nil.
func Union[T comparable](slices ...[]T) []T {
	if len(slices) == 0 {
		return nil
	}
	
	// Check if all slices are nil
	allNil := true
	for _, slice := range slices {
		if slice != nil {
			allNil = false
			break
		}
	}
	if allNil {
		return nil
	}
	
	seen := make(map[T]bool)
	var result []T
	
	for _, slice := range slices {
		for _, item := range slice {
			if !seen[item] {
				seen[item] = true
				result = append(result, item)
			}
		}
	}
	
	return result
}

// Intersection returns elements common to all input slices.
// If any slice is nil or empty, or no slices provided, returns empty slice.
func Intersection[T comparable](slices ...[]T) []T {
	if len(slices) == 0 {
		return []T{}
	}
	
	// If any slice is nil or empty, intersection is empty
	for _, slice := range slices {
		if slice == nil || len(slice) == 0 {
			return []T{}
		}
	}
	
	// Count occurrences in all slices
	counts := make(map[T]int)
	
	for _, slice := range slices {
		seen := make(map[T]bool)
		for _, item := range slice {
			if !seen[item] {
				seen[item] = true
				counts[item]++
			}
		}
	}
	
	// Elements that appear in all slices
	var result []T
	for item, count := range counts {
		if count == len(slices) {
			result = append(result, item)
		}
	}
	
	return result
}

// Diff returns three slices: elements only in first, elements only in second, elements in both.
// If either slice is nil, treats it as empty.
func Diff[T comparable](first []T, second []T) (onlyFirst []T, onlySecond []T, both []T) {
	firstSet := make(map[T]bool)
	secondSet := make(map[T]bool)
	
	// Build sets
	for _, item := range first {
		firstSet[item] = true
	}
	for _, item := range second {
		secondSet[item] = true
	}
	
	// Find elements only in first
	for _, item := range first {
		if !secondSet[item] {
			onlyFirst = append(onlyFirst, item)
			delete(firstSet, item) // Avoid duplicates
		}
	}
	
	// Find elements only in second
	for _, item := range second {
		if !firstSet[item] {
			onlySecond = append(onlySecond, item)
			delete(secondSet, item) // Avoid duplicates
		}
	}
	
	// Find elements in both
	for _, item := range first {
		if secondSet[item] {
			both = append(both, item)
			delete(secondSet, item) // Avoid duplicates
		}
	}
	
	return onlyFirst, onlySecond, both
}

// FlatMap applies function to each element and flattens the results.
// If the input slice is nil, returns nil.
func FlatMap[T, U any](slice []T, fn func(T) []U) []U {
	if slice == nil {
		return nil
	}
	
	var result []U
	for _, item := range slice {
		result = append(result, fn(item)...)
	}
	return result
}

// MinBy returns the minimum element according to the comparison function.
// Returns zero value and false for nil or empty slice.
func MinBy[T any](slice []T, less func(T, T) bool) (T, bool) {
	var zero T
	if slice == nil || len(slice) == 0 {
		return zero, false
	}
	
	min := slice[0]
	for i := 1; i < len(slice); i++ {
		if less(slice[i], min) {
			min = slice[i]
		}
	}
	return min, true
}

// MaxBy returns the maximum element according to the comparison function.
// Returns zero value and false for nil or empty slice.
func MaxBy[T any](slice []T, less func(T, T) bool) (T, bool) {
	var zero T
	if slice == nil || len(slice) == 0 {
		return zero, false
	}
	
	max := slice[0]
	for i := 1; i < len(slice); i++ {
		if less(max, slice[i]) {
			max = slice[i]
		}
	}
	return max, true
}

// SortBy returns a new slice sorted by the key function.
// If the input slice is nil, returns nil.
func SortBy[T any, K cmp.Ordered](slice []T, keyFn func(T) K) []T {
	if slice == nil {
		return nil
	}
	
	result := make([]T, len(slice))
	copy(result, slice)
	
	// Sort using the key function with Go's stable sort
	slices.SortStableFunc(result, func(a, b T) int {
		keyA, keyB := keyFn(a), keyFn(b)
		return cmp.Compare(keyA, keyB)
	})
	
	return result
}
