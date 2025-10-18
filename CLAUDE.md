# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

- **Test**: `go test ./... -v` - Run all tests with verbose output
- **Build**: `go build ./...` - Build all packages in the repository
- **Tidy Dependencies**: `go mod tidy` - Clean up module dependencies
- **Run Single Package Tests**: `go test ./slicex -v` - Run tests for specific package

## Code Architecture

This is a Go utilities library focused on slice operations and functional programming patterns. The codebase follows a clean, generic approach using Go 1.18+ generics.

### Core Components

**slicex Package** (`slicex/`):
- `Map[T, U any](slice []T, fn func(T) U) []U` - Synchronous slice transformation
- `MapConcurrent[T, U any]` - Concurrent slice transformation with configurable concurrency and error handling
  - Builder pattern API: `.Concurrency(n)`, `.CollectErrors()` 
  - Supports both fail-fast and collect-all-errors modes
  - Context-aware with proper cancellation handling
- `Filter[T any](slice []T, predicates ...func(T) bool) []T` - Filter with AND logic for multiple predicates
- `FilterNonzero[T comparable](slice []T) []T` - Remove zero values
- `Take[T any](slice []T, n int) []T` - Take first n elements
- `Skip[T any](slice []T, n int) []T` - Skip first n elements
- `Reduce[T, U any](slice []T, initial U, fn func(U, T) U) U` - Reduce slice to single value
- `Find[T any](slice []T, predicate func(T) bool) (T, int, bool)` - Find first matching element
- `Any[T any](slice []T, predicate func(T) bool) bool` - Check if any element matches
- `All[T any](slice []T, predicate func(T) bool) bool` - Check if all elements match
- `Sum[T numeric](slice []T) T` - Sum all numeric elements
- `Reverse[T any](slice []T) []T` - Reverse slice order
- `Unique[T comparable](slice []T) []T` - Remove duplicates
- `Partition[T any](slice []T, predicate func(T) bool) ([]T, []T)` - Split by predicate
- `GroupBy[T any, K comparable](slice []T, keyFn func(T) K) map[K][]T` - Group by key function
- `Chunk[T any](slice []T, size int) [][]T` - Split into fixed-size chunks
- `Union[T comparable](slices ...[]T) []T` - Combine unique elements from multiple slices
- `Intersection[T comparable](slices ...[]T) []T` - Find common elements
- `Diff[T comparable](first, second []T) (onlyFirst, onlySecond, both []T)` - Three-way diff
- `FlatMap[T, U any](slice []T, fn func(T) []U) []U` - Map and flatten results
- `MinBy[T any](slice []T, less func(T, T) bool) (T, bool)` - Find minimum by comparison
- `MaxBy[T any](slice []T, less func(T, T) bool) (T, bool)` - Find maximum by comparison
- `SortBy[T any, K cmp.Ordered](slice []T, keyFn func(T) K) []T` - Sort by key function

### Key Design Patterns

- **Generic Functions**: All functions use Go generics for type safety across different slice types
- **Nil Safety**: All functions properly handle nil slice inputs by returning nil
- **Builder Pattern**: `MapConcurrent` uses method chaining for configuration
- **Worker Pool**: Concurrent operations use a bounded worker pool pattern with configurable concurrency
- **Context Propagation**: Async operations respect context cancellation and timeouts

### Testing Strategy

- Comprehensive unit tests with table-driven patterns
- Example tests for documentation (runnable examples)
- Concurrent behavior testing including order preservation and cancellation
- Edge case coverage (nil slices, empty slices, error conditions)

Module name: `github.com/savaki/gox`
