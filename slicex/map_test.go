package slicex

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMap(t *testing.T) {
	t.Run("int to string", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		expected := []string{"1", "2", "3", "4", "5"}
		result := Map(input, func(n int) string {
			return strconv.Itoa(n)
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("string to int length", func(t *testing.T) {
		input := []string{"hello", "world", "go", "generics"}
		expected := []int{5, 5, 2, 8}
		result := Map(input, func(s string) int {
			return len(s)
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("string to uppercase", func(t *testing.T) {
		input := []string{"hello", "world"}
		expected := []string{"HELLO", "WORLD"}
		result := Map(input, func(s string) string {
			return strings.ToUpper(s)
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		input := []int{}
		result := Map(input, func(n int) string {
			return strconv.Itoa(n)
		})
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		result := Map(input, func(n int) string {
			return strconv.Itoa(n)
		})
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
	
	t.Run("complex transformation", func(t *testing.T) {
		type Person struct {
			Name string
			Age  int
		}
		
		input := []Person{
			{"Alice", 30},
			{"Bob", 25},
			{"Charlie", 35},
		}
		
		expected := []string{"Alice (30)", "Bob (25)", "Charlie (35)"}
		result := Map(input, func(p Person) string {
			return p.Name + " (" + strconv.Itoa(p.Age) + ")"
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}

func TestMapConcurrent(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		inputs := []int{1, 2, 3, 4, 5}
		expected := []string{"1", "2", "3", "4", "5"}
		
		results, err := MapConcurrent(func(ctx context.Context, n int) (string, error) {
			return strconv.Itoa(n), nil
		}).DoValues(context.Background(), inputs...)
		
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		
		if !reflect.DeepEqual(results, expected) {
			t.Errorf("Expected %v, got %v", expected, results)
		}
	})
	
	t.Run("order preservation", func(t *testing.T) {
		inputs := []int{1, 2, 3, 4, 5}
		
		results, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			time.Sleep(time.Duration(6-n) * time.Millisecond)
			return n * 10, nil
		}).DoValues(context.Background(), inputs...)
		
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		
		expected := []int{10, 20, 30, 40, 50}
		if !reflect.DeepEqual(results, expected) {
			t.Errorf("Order not preserved. Expected %v, got %v", expected, results)
		}
	})
	
	t.Run("concurrency control", func(t *testing.T) {
		var activeCount int64
		var maxActive int64
		inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		
		_, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			current := atomic.AddInt64(&activeCount, 1)
			defer atomic.AddInt64(&activeCount, -1)
			
			for {
				max := atomic.LoadInt64(&maxActive)
				if current <= max || atomic.CompareAndSwapInt64(&maxActive, max, current) {
					break
				}
			}
			
			time.Sleep(10 * time.Millisecond)
			return n, nil
		}).Concurrency(3).DoValues(context.Background(), inputs...)
		
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		
		if maxActive > 3 {
			t.Errorf("Concurrency limit exceeded. Max active: %d, limit: 3", maxActive)
		}
	})
	
	t.Run("fail fast mode", func(t *testing.T) {
		inputs := []int{1, 2, 3, 4, 5}
		
		_, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			if n == 3 {
				return 0, errors.New("error at 3")
			}
			return n, nil
		}).DoValues(context.Background(), inputs...)
		
		if err == nil {
			t.Error("Expected error but got nil")
		}
		
		if err.Error() != "error at 3" {
			t.Errorf("Expected 'error at 3', got '%v'", err)
		}
	})
	
	t.Run("collect errors mode", func(t *testing.T) {
		inputs := []int{1, 2, 3, 4, 5}
		
		results, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			if n == 3 || n == 5 {
				return 0, errors.New("error at " + strconv.Itoa(n))
			}
			return n * 10, nil
		}).CollectErrors().DoValues(context.Background(), inputs...)
		
		if err == nil {
			t.Error("Expected error but got nil")
		}
		
		if !strings.Contains(err.Error(), "error at 3") || !strings.Contains(err.Error(), "error at 5") {
			t.Errorf("Expected both errors in joined error, got: %v", err)
		}
		
		expected := []int{10, 20, 0, 40, 0}
		if !reflect.DeepEqual(results, expected) {
			t.Errorf("Expected %v, got %v", expected, results)
		}
	})
	
	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		inputs := []int{1, 2, 3, 4, 5}
		
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		
		_, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return n, nil
		}).DoValues(ctx, inputs...)
		
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
	
	t.Run("empty input", func(t *testing.T) {
		var inputs []int
		
		results, err := MapConcurrent(func(ctx context.Context, n int) (string, error) {
			return strconv.Itoa(n), nil
		}).DoValues(context.Background(), inputs...)
		
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		
		if len(results) != 0 {
			t.Errorf("Expected empty slice, got %v", results)
		}
	})
	
	t.Run("chaining options", func(t *testing.T) {
		inputs := []int{1, 2, 3}
		
		_, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			if n == 2 {
				return 0, errors.New("error at 2")
			}
			return n, nil
		}).Concurrency(2).CollectErrors().DoValues(context.Background(), inputs...)
		
		if err == nil {
			t.Error("Expected error but got nil")
		}
		
		if !strings.Contains(err.Error(), "error at 2") {
			t.Errorf("Expected error message containing 'error at 2', got: %v", err)
		}
	})
	
	t.Run("fail fast cancels remaining work", func(t *testing.T) {
		var processedCount int64
		inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		
		_, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			atomic.AddInt64(&processedCount, 1)
			time.Sleep(50 * time.Millisecond) // Give time for other goroutines to start
			
			if n == 3 {
				return 0, errors.New("error at 3")
			}
			
			// Check if context was cancelled
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
			}
			
			return n, nil
		}).Concurrency(5).DoValues(context.Background(), inputs...)
		
		if err == nil {
			t.Error("Expected error but got nil")
		}
		
		// Should have processed fewer items due to cancellation
		processed := atomic.LoadInt64(&processedCount)
		if processed >= int64(len(inputs)) {
			t.Errorf("Expected fewer than %d items to be processed due to cancellation, got %d", len(inputs), processed)
		}
		
		t.Logf("Processed %d out of %d items before cancellation", processed, len(inputs))
	})
	
	t.Run("context cancellation during work sending", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		inputs := make([]int, 1000) // Large number of inputs
		for i := range inputs {
			inputs[i] = i
		}
		
		// Cancel immediately to test work sender cancellation
		cancel()
		
		_, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			time.Sleep(1 * time.Millisecond) // Small delay
			return n, nil
		}).Concurrency(1).DoValues(ctx, inputs...)
		
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
	
	t.Run("collect errors with some nil errors", func(t *testing.T) {
		inputs := []int{1, 2, 3, 4, 5}
		
		results, err := MapConcurrent(func(ctx context.Context, n int) (int, error) {
			if n == 2 || n == 4 {
				return 0, errors.New("error at " + strconv.Itoa(n))
			}
			return n * 10, nil
		}).CollectErrors().DoValues(context.Background(), inputs...)
		
		if err == nil {
			t.Error("Expected error but got nil")
		}
		
		expected := []int{10, 0, 30, 0, 50}
		if !reflect.DeepEqual(results, expected) {
			t.Errorf("Expected %v, got %v", expected, results)
		}
	})
}

func TestFilter(t *testing.T) {
	t.Run("single predicate", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		expected := []int{2, 4, 6, 8, 10}
		
		result := Filter(input, func(n int) bool {
			return n%2 == 0
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("multiple predicates AND", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		expected := []int{6, 12} // divisible by both 2 and 3
		
		result := Filter(input, 
			func(n int) bool { return n%2 == 0 }, // even
			func(n int) bool { return n%3 == 0 }, // divisible by 3
		)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("no predicates", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		expected := []int{1, 2, 3, 4, 5}
		
		result := Filter(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
		
		// Should be a copy, not the same slice
		if &result[0] == &input[0] {
			t.Error("Expected result to be a copy, not the same slice")
		}
	})
	
	t.Run("no matches", func(t *testing.T) {
		input := []int{1, 3, 5, 7, 9}
		var expected []int
		
		result := Filter(input, func(n int) bool {
			return n%2 == 0
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("all match", func(t *testing.T) {
		input := []int{2, 4, 6, 8, 10}
		expected := []int{2, 4, 6, 8, 10}
		
		result := Filter(input, func(n int) bool {
			return n%2 == 0
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []int
		var expected []int
		
		result := Filter(input, func(n int) bool {
			return n%2 == 0
		})
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		
		result := Filter(input, func(n int) bool {
			return n%2 == 0
		})
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
	
	t.Run("string filtering", func(t *testing.T) {
		input := []string{"apple", "banana", "cherry", "date", "elderberry"}
		expected := []string{"cherry", "elderberry"} // length > 5 and contains 'e'
		
		result := Filter(input,
			func(s string) bool { return len(s) > 5 },
			func(s string) bool { return strings.Contains(s, "e") },
		)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("three predicates", func(t *testing.T) {
		input := []int{1, 6, 12, 18, 24, 30, 36, 42}
		expected := []int{12, 24, 36} // divisible by 2, 3, and 4
		
		result := Filter(input,
			func(n int) bool { return n%2 == 0 },
			func(n int) bool { return n%3 == 0 },
			func(n int) bool { return n%4 == 0 },
		)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}

func TestFilterNonzero(t *testing.T) {
	t.Run("string slice", func(t *testing.T) {
		input := []string{"hello", "", "world", "", "go", ""}
		expected := []string{"hello", "world", "go"}
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("int slice", func(t *testing.T) {
		input := []int{1, 0, 3, 0, 5, 0}
		expected := []int{1, 3, 5}
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("float slice", func(t *testing.T) {
		input := []float64{1.1, 0.0, 2.2, 0.0, 3.3}
		expected := []float64{1.1, 2.2, 3.3}
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("bool slice", func(t *testing.T) {
		input := []bool{true, false, true, false}
		expected := []bool{true, true}
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("all zero values", func(t *testing.T) {
		input := []string{"", "", ""}
		var expected []string
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("no zero values", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		expected := []int{1, 2, 3, 4, 5}
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []string
		var expected []string
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []string
		input = nil
		
		result := FilterNonzero(input)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
	
	t.Run("pointer slice", func(t *testing.T) {
		val1 := 42
		val2 := 84
		input := []*int{&val1, nil, &val2, nil}
		expected := []*int{&val1, &val2}
		
		result := FilterNonzero(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}

func TestTake(t *testing.T) {
	t.Run("take fewer than available", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		expected := []int{1, 2, 3}
		result := Take(input, 3)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
		
		// Should be a copy, not the same slice
		if len(result) > 0 && len(input) > 0 && &result[0] == &input[0] {
			t.Error("Expected result to be a copy, not the same slice")
		}
	})
	
	t.Run("take more than available", func(t *testing.T) {
		input := []int{1, 2, 3}
		expected := []int{1, 2, 3}
		result := Take(input, 10)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
		
		// Should be a copy, not the same slice
		if len(result) > 0 && len(input) > 0 && &result[0] == &input[0] {
			t.Error("Expected result to be a copy, not the same slice")
		}
	})
	
	t.Run("take exact amount", func(t *testing.T) {
		input := []int{1, 2, 3}
		expected := []int{1, 2, 3}
		result := Take(input, 3)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
		
		// Should be a copy, not the same slice
		if len(result) > 0 && len(input) > 0 && &result[0] == &input[0] {
			t.Error("Expected result to be a copy, not the same slice")
		}
	})
	
	t.Run("take zero", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := Take(input, 0)
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("take negative", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := Take(input, -5)
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []int
		var expected []int
		result := Take(input, 3)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Take(input, 3)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
	
	t.Run("string slice", func(t *testing.T) {
		input := []string{"apple", "banana", "cherry", "date"}
		expected := []string{"apple", "banana"}
		result := Take(input, 2)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("complex type", func(t *testing.T) {
		type Person struct {
			Name string
			Age  int
		}
		
		input := []Person{
			{"Alice", 30},
			{"Bob", 25},
			{"Charlie", 35},
			{"David", 40},
		}
		
		expected := []Person{
			{"Alice", 30},
			{"Bob", 25},
		}
		
		result := Take(input, 2)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}

func TestSkip(t *testing.T) {
	t.Run("skip fewer than available", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		expected := []int{3, 4, 5}
		result := Skip(input, 2)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("skip more than available", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := Skip(input, 10)
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("skip zero", func(t *testing.T) {
		input := []int{1, 2, 3}
		expected := []int{1, 2, 3}
		result := Skip(input, 0)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
		
		// Should be a copy
		if len(result) > 0 && len(input) > 0 && &result[0] == &input[0] {
			t.Error("Expected result to be a copy, not the same slice")
		}
	})
	
	t.Run("skip negative", func(t *testing.T) {
		input := []int{1, 2, 3}
		expected := []int{1, 2, 3}
		result := Skip(input, -5)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Skip(input, 2)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestReduce(t *testing.T) {
	t.Run("sum integers", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		result := Reduce(input, 0, func(acc, item int) int {
			return acc + item
		})
		
		if result != 15 {
			t.Errorf("Expected 15, got %d", result)
		}
	})
	
	t.Run("concatenate strings", func(t *testing.T) {
		input := []string{"hello", " ", "world"}
		result := Reduce(input, "", func(acc, item string) string {
			return acc + item
		})
		
		if result != "hello world" {
			t.Errorf("Expected 'hello world', got '%s'", result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Reduce(input, 42, func(acc, item int) int {
			return acc + item
		})
		
		if result != 42 {
			t.Errorf("Expected 42, got %d", result)
		}
	})
}

func TestFind(t *testing.T) {
	t.Run("element found", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		value, index, found := Find(input, func(n int) bool {
			return n == 3
		})
		
		if !found {
			t.Error("Expected to find element")
		}
		if value != 3 {
			t.Errorf("Expected value 3, got %d", value)
		}
		if index != 2 {
			t.Errorf("Expected index 2, got %d", index)
		}
	})
	
	t.Run("element not found", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		value, index, found := Find(input, func(n int) bool {
			return n == 10
		})
		
		if found {
			t.Error("Expected not to find element")
		}
		if value != 0 {
			t.Errorf("Expected zero value, got %d", value)
		}
		if index != -1 {
			t.Errorf("Expected index -1, got %d", index)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		value, index, found := Find(input, func(n int) bool {
			return n == 3
		})
		
		if found {
			t.Error("Expected not to find element")
		}
		if value != 0 {
			t.Errorf("Expected zero value, got %d", value)
		}
		if index != -1 {
			t.Errorf("Expected index -1, got %d", index)
		}
	})
}

func TestAny(t *testing.T) {
	t.Run("some match", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		result := Any(input, func(n int) bool {
			return n > 3
		})
		
		if !result {
			t.Error("Expected true")
		}
	})
	
	t.Run("none match", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		result := Any(input, func(n int) bool {
			return n > 10
		})
		
		if result {
			t.Error("Expected false")
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Any(input, func(n int) bool {
			return n > 0
		})
		
		if result {
			t.Error("Expected false")
		}
	})
}

func TestAll(t *testing.T) {
	t.Run("all match", func(t *testing.T) {
		input := []int{2, 4, 6, 8}
		result := All(input, func(n int) bool {
			return n%2 == 0
		})
		
		if !result {
			t.Error("Expected true")
		}
	})
	
	t.Run("some don't match", func(t *testing.T) {
		input := []int{2, 4, 5, 8}
		result := All(input, func(n int) bool {
			return n%2 == 0
		})
		
		if result {
			t.Error("Expected false")
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []int
		result := All(input, func(n int) bool {
			return n > 10
		})
		
		if !result {
			t.Error("Expected true (vacuous truth)")
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := All(input, func(n int) bool {
			return n > 10
		})
		
		if !result {
			t.Error("Expected true (vacuous truth)")
		}
	})
}

func TestSum(t *testing.T) {
	t.Run("sum integers", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		result := Sum(input)
		
		if result != 15 {
			t.Errorf("Expected 15, got %d", result)
		}
	})
	
	t.Run("sum floats", func(t *testing.T) {
		input := []float64{1.1, 2.2, 3.3}
		result := Sum(input)
		
		expected := 6.6
		if result < expected-0.0001 || result > expected+0.0001 {
			t.Errorf("Expected %f, got %f", expected, result)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []int
		result := Sum(input)
		
		if result != 0 {
			t.Errorf("Expected 0, got %d", result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Sum(input)
		
		if result != 0 {
			t.Errorf("Expected 0, got %d", result)
		}
	})
}

func TestReverse(t *testing.T) {
	t.Run("reverse integers", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		expected := []int{5, 4, 3, 2, 1}
		result := Reverse(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("single element", func(t *testing.T) {
		input := []int{42}
		expected := []int{42}
		result := Reverse(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Reverse(input)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestUnique(t *testing.T) {
	t.Run("remove duplicates", func(t *testing.T) {
		input := []int{1, 2, 2, 3, 3, 3, 4}
		expected := []int{1, 2, 3, 4}
		result := Unique(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("no duplicates", func(t *testing.T) {
		input := []string{"a", "b", "c"}
		expected := []string{"a", "b", "c"}
		result := Unique(input)
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Unique(input)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestPartition(t *testing.T) {
	t.Run("even odd", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5, 6}
		evens, odds := Partition(input, func(n int) bool {
			return n%2 == 0
		})
		
		expectedEvens := []int{2, 4, 6}
		expectedOdds := []int{1, 3, 5}
		
		if !reflect.DeepEqual(evens, expectedEvens) {
			t.Errorf("Expected evens %v, got %v", expectedEvens, evens)
		}
		if !reflect.DeepEqual(odds, expectedOdds) {
			t.Errorf("Expected odds %v, got %v", expectedOdds, odds)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		matching, nonMatching := Partition(input, func(n int) bool {
			return n%2 == 0
		})
		
		if matching != nil {
			t.Errorf("Expected nil matching, got %v", matching)
		}
		if nonMatching != nil {
			t.Errorf("Expected nil non-matching, got %v", nonMatching)
		}
	})
}

func TestGroupBy(t *testing.T) {
	t.Run("group by length", func(t *testing.T) {
		input := []string{"a", "bb", "cc", "ddd"}
		result := GroupBy(input, func(s string) int {
			return len(s)
		})
		
		expected := map[int][]string{
			1: {"a"},
			2: {"bb", "cc"},
			3: {"ddd"},
		}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []string
		input = nil
		result := GroupBy(input, func(s string) int {
			return len(s)
		})
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestChunk(t *testing.T) {
	t.Run("exact chunks", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5, 6}
		result := Chunk(input, 2)
		expected := [][]int{{1, 2}, {3, 4}, {5, 6}}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("partial last chunk", func(t *testing.T) {
		input := []int{1, 2, 3, 4, 5}
		result := Chunk(input, 2)
		expected := [][]int{{1, 2}, {3, 4}, {5}}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("chunk size 1", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := Chunk(input, 1)
		expected := [][]int{{1}, {2}, {3}}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("negative chunk size", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := Chunk(input, -1)
		
		if len(result) != 0 {
			t.Errorf("Expected empty result, got %v", result)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []int
		result := Chunk(input, 2)
		
		if len(result) != 0 {
			t.Errorf("Expected empty result, got %v", result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		result := Chunk(input, 2)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestUnion(t *testing.T) {
	t.Run("basic union", func(t *testing.T) {
		slice1 := []int{1, 2, 3}
		slice2 := []int{3, 4, 5}
		result := Union(slice1, slice2)
		expected := []int{1, 2, 3, 4, 5}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("no slices provided", func(t *testing.T) {
		result := Union[int]()
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
	
	t.Run("mix of nil and non-nil slices", func(t *testing.T) {
		var slice1 []int
		slice1 = nil
		slice2 := []int{1, 2, 3}
		var slice3 []int
		slice3 = nil
		slice4 := []int{3, 4, 5}
		
		result := Union(slice1, slice2, slice3, slice4)
		expected := []int{1, 2, 3, 4, 5}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("empty slices", func(t *testing.T) {
		slice1 := []int{}
		slice2 := []int{}
		result := Union(slice1, slice2)
		
		if len(result) != 0 {
			t.Errorf("Expected empty result, got %v", result)
		}
	})
	
	t.Run("all nil slices", func(t *testing.T) {
		var slice1, slice2 []int
		slice1 = nil
		slice2 = nil
		result := Union(slice1, slice2)
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestIntersection(t *testing.T) {
	t.Run("basic intersection", func(t *testing.T) {
		slice1 := []int{1, 2, 3, 4}
		slice2 := []int{3, 4, 5, 6}
		result := Intersection(slice1, slice2)
		
		// Check if result contains expected elements
		if len(result) != 2 {
			t.Errorf("Expected 2 elements, got %v", result)
		}
		
		found3, found4 := false, false
		for _, v := range result {
			if v == 3 {
				found3 = true
			}
			if v == 4 {
				found4 = true
			}
		}
		if !found3 || !found4 {
			t.Errorf("Expected to find 3 and 4 in result %v", result)
		}
	})
	
	t.Run("no intersection", func(t *testing.T) {
		slice1 := []int{1, 2}
		slice2 := []int{3, 4}
		result := Intersection(slice1, slice2)
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("no slices provided", func(t *testing.T) {
		result := Intersection[int]()
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("one empty slice", func(t *testing.T) {
		slice1 := []int{1, 2, 3}
		slice2 := []int{}
		result := Intersection(slice1, slice2)
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("one nil slice", func(t *testing.T) {
		slice1 := []int{1, 2, 3}
		var slice2 []int
		slice2 = nil
		result := Intersection(slice1, slice2)
		
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})
	
	t.Run("three slices intersection", func(t *testing.T) {
		slice1 := []int{1, 2, 3, 4, 5}
		slice2 := []int{2, 3, 4, 5, 6}
		slice3 := []int{3, 4, 5, 6, 7}
		result := Intersection(slice1, slice2, slice3)
		
		// Check if result contains expected elements
		if len(result) != 3 {
			t.Errorf("Expected 3 elements, got %v", result)
		}
		
		found3, found4, found5 := false, false, false
		for _, v := range result {
			if v == 3 {
				found3 = true
			}
			if v == 4 {
				found4 = true
			}
			if v == 5 {
				found5 = true
			}
		}
		if !found3 || !found4 || !found5 {
			t.Errorf("Expected to find 3, 4, and 5 in result %v", result)
		}
	})
	
	t.Run("slices with duplicates", func(t *testing.T) {
		slice1 := []int{1, 1, 2, 2, 3}
		slice2 := []int{2, 2, 3, 3, 4}
		result := Intersection(slice1, slice2)
		
		// Check if result contains expected elements
		if len(result) != 2 {
			t.Errorf("Expected 2 elements, got %v", result)
		}
		
		found2, found3 := false, false
		for _, v := range result {
			if v == 2 {
				found2 = true
			}
			if v == 3 {
				found3 = true
			}
		}
		if !found2 || !found3 {
			t.Errorf("Expected to find 2 and 3 in result %v", result)
		}
	})
}

func TestDiff(t *testing.T) {
	t.Run("basic diff", func(t *testing.T) {
		first := []int{1, 2, 3, 4}
		second := []int{3, 4, 5, 6}
		onlyFirst, onlySecond, both := Diff(first, second)
		
		expectedOnlyFirst := []int{1, 2}
		expectedOnlySecond := []int{5, 6}
		expectedBoth := []int{3, 4}
		
		if !reflect.DeepEqual(onlyFirst, expectedOnlyFirst) {
			t.Errorf("Expected onlyFirst %v, got %v", expectedOnlyFirst, onlyFirst)
		}
		if !reflect.DeepEqual(onlySecond, expectedOnlySecond) {
			t.Errorf("Expected onlySecond %v, got %v", expectedOnlySecond, onlySecond)
		}
		if !reflect.DeepEqual(both, expectedBoth) {
			t.Errorf("Expected both %v, got %v", expectedBoth, both)
		}
	})
	
	t.Run("with nil slices", func(t *testing.T) {
		var first []int
		first = nil
		second := []int{1, 2, 3}
		onlyFirst, onlySecond, both := Diff(first, second)
		
		if len(onlyFirst) != 0 {
			t.Errorf("Expected empty onlyFirst, got %v", onlyFirst)
		}
		
		expectedOnlySecond := []int{1, 2, 3}
		if !reflect.DeepEqual(onlySecond, expectedOnlySecond) {
			t.Errorf("Expected onlySecond %v, got %v", expectedOnlySecond, onlySecond)
		}
		
		if len(both) != 0 {
			t.Errorf("Expected empty both, got %v", both)
		}
	})
	
	t.Run("with duplicates", func(t *testing.T) {
		first := []int{1, 1, 2, 2, 3}
		second := []int{2, 2, 3, 3, 4}
		onlyFirst, onlySecond, both := Diff(first, second)
		
		// The function should preserve the original order and duplicates
		// Elements only in first: 1 appears twice, so both should be included
		expectedOnlyFirst := []int{1, 1}
		
		if !reflect.DeepEqual(onlyFirst, expectedOnlyFirst) {
			t.Errorf("Expected onlyFirst %v, got %v", expectedOnlyFirst, onlyFirst)
		}
		// Note: second slice has 4 appearing twice, but onlySecond will only get first occurrence
		if len(onlySecond) == 0 || onlySecond[0] != 4 {
			t.Errorf("Expected onlySecond to contain 4, got %v", onlySecond)
		}
		// Note: both will contain first occurrences from first slice
		if len(both) == 0 || both[0] != 2 {
			t.Errorf("Expected both to contain 2, got %v", both)
		}
	})
}

func TestFlatMap(t *testing.T) {
	t.Run("basic flat map", func(t *testing.T) {
		input := []string{"hello", "world"}
		result := FlatMap(input, func(s string) []string {
			return []string{s, s}
		})
		expected := []string{"hello", "hello", "world", "world"}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []string
		input = nil
		result := FlatMap(input, func(s string) []string {
			return []string{s}
		})
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}

func TestMinBy(t *testing.T) {
	t.Run("find minimum", func(t *testing.T) {
		input := []int{5, 2, 8, 1, 9}
		min, found := MinBy(input, func(a, b int) bool {
			return a < b
		})
		
		if !found {
			t.Error("Expected to find minimum")
		}
		if min != 1 {
			t.Errorf("Expected 1, got %d", min)
		}
	})
	
	t.Run("empty slice", func(t *testing.T) {
		var input []int
		_, found := MinBy(input, func(a, b int) bool {
			return a < b
		})
		
		if found {
			t.Error("Expected not to find minimum in empty slice")
		}
	})
}

func TestMaxBy(t *testing.T) {
	t.Run("find maximum", func(t *testing.T) {
		input := []int{5, 2, 8, 1, 9}
		max, found := MaxBy(input, func(a, b int) bool {
			return a < b
		})
		
		if !found {
			t.Error("Expected to find maximum")
		}
		if max != 9 {
			t.Errorf("Expected 9, got %d", max)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []int
		input = nil
		_, found := MaxBy(input, func(a, b int) bool {
			return a < b
		})
		
		if found {
			t.Error("Expected not to find maximum in nil slice")
		}
	})
}

func TestSortBy(t *testing.T) {
	t.Run("sort by length", func(t *testing.T) {
		input := []string{"hello", "go", "world", "a"}
		result := SortBy(input, func(s string) int {
			return len(s)
		})
		expected := []string{"a", "go", "hello", "world"}
		
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
	
	t.Run("nil slice", func(t *testing.T) {
		var input []string
		input = nil
		result := SortBy(input, func(s string) int {
			return len(s)
		})
		
		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})
}