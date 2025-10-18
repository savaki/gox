package slicex_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/savaki/gox/slicex"
)

func ExampleMap() {
	// Convert integers to strings
	numbers := []int{1, 2, 3, 4, 5}
	strings := slicex.Map(numbers, func(n int) string {
		return strconv.Itoa(n)
	})
	fmt.Println(strings)
	// Output: [1 2 3 4 5]
}

func ExampleMap_stringToUpper() {
	// Convert strings to uppercase
	words := []string{"hello", "world", "go"}
	upper := slicex.Map(words, func(s string) string {
		return strings.ToUpper(s)
	})
	fmt.Println(upper)
	// Output: [HELLO WORLD GO]
}

func ExampleMap_stringLength() {
	// Get length of each string
	words := []string{"hello", "go", "generics"}
	lengths := slicex.Map(words, func(s string) int {
		return len(s)
	})
	fmt.Println(lengths)
	// Output: [5 2 8]
}

func ExampleMapConcurrent() {
	// Transform integers concurrently
	inputs := []int{1, 2, 3, 4, 5}
	
	results, err := slicex.MapConcurrent(func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("value_%d", n), nil
	}).DoValues(context.Background(), inputs...)
	
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Println(results)
	// Output: [value_1 value_2 value_3 value_4 value_5]
}

func ExampleMapConcurrent_withConcurrency() {
	// Limit concurrency to 2 goroutines
	inputs := []int{1, 2, 3, 4, 5}
	
	results, err := slicex.MapConcurrent(func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	}).Concurrency(2).DoValues(context.Background(), inputs...)
	
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Println(results)
	// Output: [1 4 9 16 25]
}

func ExampleMapConcurrent_collectErrors() {
	// Collect all errors instead of failing fast
	inputs := []int{1, 2, 3, 4, 5}
	
	results, err := slicex.MapConcurrent(func(ctx context.Context, n int) (int, error) {
		if n%2 == 0 {
			return 0, fmt.Errorf("error processing %d", n)
		}
		return n * 10, nil
	}).CollectErrors().DoValues(context.Background(), inputs...)
	
	if err != nil {
		fmt.Printf("Errors occurred: %v\n", err)
	}
	
	fmt.Println(results)
	// Output: 
	// Errors occurred: error processing 2
	// error processing 4
	// [10 0 30 0 50]
}

func ExampleFilter() {
	// Filter even numbers
	numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	evens := slicex.Filter(numbers, func(n int) bool {
		return n%2 == 0
	})
	fmt.Println(evens)
	// Output: [2 4 6 8 10]
}

func ExampleFilter_multiplePredicates() {
	// Filter numbers divisible by both 2 and 3
	numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	result := slicex.Filter(numbers,
		func(n int) bool { return n%2 == 0 }, // even
		func(n int) bool { return n%3 == 0 }, // divisible by 3
	)
	fmt.Println(result)
	// Output: [6 12]
}

func ExampleFilter_strings() {
	// Filter strings longer than 5 characters that contain 'e'
	words := []string{"apple", "banana", "cherry", "date", "elderberry"}
	result := slicex.Filter(words,
		func(s string) bool { return len(s) > 5 },
		func(s string) bool { return strings.Contains(s, "e") },
	)
	fmt.Println(result)
	// Output: [cherry elderberry]
}

func ExampleFilterNonzero() {
	// Filter out empty strings
	words := []string{"hello", "", "world", "", "go"}
	result := slicex.FilterNonzero(words)
	fmt.Println(result)
	// Output: [hello world go]
}

func ExampleFilterNonzero_integers() {
	// Filter out zero integers
	numbers := []int{1, 0, 3, 0, 5}
	result := slicex.FilterNonzero(numbers)
	fmt.Println(result)
	// Output: [1 3 5]
}

func ExampleFilterNonzero_booleans() {
	// Filter out false values (zero value for bool)
	flags := []bool{true, false, true, false}
	result := slicex.FilterNonzero(flags)
	fmt.Println(result)
	// Output: [true true]
}

func ExampleTake() {
	// Take first 3 elements from slice
	numbers := []int{1, 2, 3, 4, 5, 6}
	result := slicex.Take(numbers, 3)
	fmt.Println(result)
	// Output: [1 2 3]
}

func ExampleTake_moreThanAvailable() {
	// Take more elements than available
	numbers := []int{1, 2, 3}
	result := slicex.Take(numbers, 10)
	fmt.Println(result)
	// Output: [1 2 3]
}

func ExampleTake_strings() {
	// Take first 2 strings
	words := []string{"apple", "banana", "cherry", "date"}
	result := slicex.Take(words, 2)
	fmt.Println(result)
	// Output: [apple banana]
}

func ExampleTake_empty() {
	// Take from empty slice
	var numbers []int
	result := slicex.Take(numbers, 3)
	fmt.Println(result)
	fmt.Println("Length:", len(result))
	// Output: []
	// Length: 0
}

func ExampleSkip() {
	// Skip first 2 elements
	numbers := []int{1, 2, 3, 4, 5}
	result := slicex.Skip(numbers, 2)
	fmt.Println(result)
	// Output: [3 4 5]
}

func ExampleReduce() {
	// Sum all numbers
	numbers := []int{1, 2, 3, 4, 5}
	sum := slicex.Reduce(numbers, 0, func(acc, n int) int {
		return acc + n
	})
	fmt.Println(sum)
	// Output: 15
}

func ExampleReduce_strings() {
	// Concatenate strings
	words := []string{"Hello", " ", "world", "!"}
	result := slicex.Reduce(words, "", func(acc, word string) string {
		return acc + word
	})
	fmt.Println(result)
	// Output: Hello world!
}

func ExampleFind() {
	// Find first even number
	numbers := []int{1, 3, 4, 5, 6}
	value, index, found := slicex.Find(numbers, func(n int) bool {
		return n%2 == 0
	})
	fmt.Printf("Found: %t, Value: %d, Index: %d\n", found, value, index)
	// Output: Found: true, Value: 4, Index: 2
}

func ExampleAny() {
	// Check if any number is greater than 3
	numbers := []int{1, 2, 3, 4, 5}
	result := slicex.Any(numbers, func(n int) bool {
		return n > 3
	})
	fmt.Println(result)
	// Output: true
}

func ExampleAll() {
	// Check if all numbers are positive
	numbers := []int{1, 2, 3, 4, 5}
	result := slicex.All(numbers, func(n int) bool {
		return n > 0
	})
	fmt.Println(result)
	// Output: true
}

func ExampleSum() {
	// Sum integers
	numbers := []int{1, 2, 3, 4, 5}
	result := slicex.Sum(numbers)
	fmt.Println(result)
	// Output: 15
}

func ExampleSum_floats() {
	// Sum floats
	numbers := []float64{1.1, 2.2, 3.3}
	result := slicex.Sum(numbers)
	fmt.Printf("%.1f\n", result)
	// Output: 6.6
}

func ExampleReverse() {
	// Reverse slice
	numbers := []int{1, 2, 3, 4, 5}
	result := slicex.Reverse(numbers)
	fmt.Println(result)
	// Output: [5 4 3 2 1]
}

func ExampleUnique() {
	// Remove duplicates
	numbers := []int{1, 2, 2, 3, 3, 3, 4}
	result := slicex.Unique(numbers)
	fmt.Println(result)
	// Output: [1 2 3 4]
}

func ExamplePartition() {
	// Separate even and odd numbers
	numbers := []int{1, 2, 3, 4, 5, 6}
	evens, odds := slicex.Partition(numbers, func(n int) bool {
		return n%2 == 0
	})
	fmt.Println("Evens:", evens)
	fmt.Println("Odds:", odds)
	// Output: Evens: [2 4 6]
	// Odds: [1 3 5]
}

func ExampleGroupBy() {
	// Group strings by length
	words := []string{"a", "bb", "cc", "ddd"}
	result := slicex.GroupBy(words, func(s string) int {
		return len(s)
	})
	fmt.Println("Length 1:", result[1])
	fmt.Println("Length 2:", result[2])
	fmt.Println("Length 3:", result[3])
	// Output: Length 1: [a]
	// Length 2: [bb cc]
	// Length 3: [ddd]
}

func ExampleChunk() {
	// Split into chunks of 2
	numbers := []int{1, 2, 3, 4, 5, 6}
	result := slicex.Chunk(numbers, 2)
	fmt.Println(result)
	// Output: [[1 2] [3 4] [5 6]]
}

func ExampleUnion() {
	// Combine unique elements
	slice1 := []int{1, 2, 3}
	slice2 := []int{3, 4, 5}
	result := slicex.Union(slice1, slice2)
	fmt.Println(result)
	// Output: [1 2 3 4 5]
}

func ExampleDiff() {
	// Find differences between slices
	first := []int{1, 2, 3, 4}
	second := []int{3, 4, 5, 6}
	onlyFirst, onlySecond, both := slicex.Diff(first, second)
	fmt.Println("Only in first:", onlyFirst)
	fmt.Println("Only in second:", onlySecond)
	fmt.Println("In both:", both)
	// Output: Only in first: [1 2]
	// Only in second: [5 6]
	// In both: [3 4]
}

func ExampleFlatMap() {
	// Duplicate each string
	words := []string{"hello", "world"}
	result := slicex.FlatMap(words, func(s string) []string {
		return []string{s, s}
	})
	fmt.Println(result)
	// Output: [hello hello world world]
}

func ExampleMinBy() {
	// Find shortest string
	words := []string{"hello", "go", "world", "a"}
	shortest, found := slicex.MinBy(words, func(a, b string) bool {
		return len(a) < len(b)
	})
	fmt.Printf("Found: %t, Shortest: %s\n", found, shortest)
	// Output: Found: true, Shortest: a
}

func ExampleMaxBy() {
	// Find longest string
	words := []string{"hello", "go", "world", "a"}
	longest, found := slicex.MaxBy(words, func(a, b string) bool {
		return len(a) < len(b)
	})
	fmt.Printf("Found: %t, Longest: %s\n", found, longest)
	// Output: Found: true, Longest: hello
}

func ExampleSortBy() {
	// Sort strings by length
	words := []string{"hello", "go", "world", "a"}
	result := slicex.SortBy(words, func(s string) int {
		return len(s)
	})
	fmt.Println(result)
	// Output: [a go hello world]
}
