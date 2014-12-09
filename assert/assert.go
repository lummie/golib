// Package assert provides an extension to the testing package to allow assertions to be added to tests
// For example assert.Equal, assert.NotEqual, assert.Nil
package assert

import (
	"fmt"
	"reflect"
	"testing"
)

// Checks that expected and actual are equal, and if not causes a standard testing Error on *t
func Equal(t *testing.T, actual, expected interface{}, messages ...interface{}) {
	if !areEqual(expected, actual) {
		t.Errorf("Equal -  expected[%v](%v) actual[%v](%v) <<%v", expected, reflect.TypeOf(expected), actual, reflect.TypeOf(actual), messages)
	}
}

// Checks that expected and actual are NOT equal, and if not causes a standard testing Error on *t
func NotEqual(t *testing.T, actual, expected interface{}, messages ...interface{}) {
	if areEqual(expected, actual) {
		t.Errorf("NotEqual -  expected[%v](%v) actual[%v](%v) <<%s", expected, reflect.TypeOf(expected), actual, reflect.TypeOf(actual), messages)
	}
}

// Checks that actual is nil, and if not causes a standard testing Error on *t
func Nil(t *testing.T, actual interface{}, messages ...interface{}) {
	if !areEqual(nil, actual) {
		t.Errorf("Expected nil, actual[%v] :%s", actual, messages)
	}
}

// Checks that actual is not nil, and if not causes a standard testing Error on *t
func NotNil(t *testing.T, actual interface{}, messages ...interface{}) {
	if areEqual(nil, actual) {
		t.Errorf("Expected not nil, actual[%v] :%s", actual, messages)
	}
}

// Checks that the supplied expected and actual objects are equal
// this code is a copy of the ObjectsAreEqual method from :
// 		https://github.com/stretchr/testify/blob/master/assert/assertions.go
//		Copyright (c) 2012 - 2013 Mat Ryer and Tyler Bunnell
func areEqual(expected, actual interface{}) bool {
	// from github.com/stretchr/testify/assertions.go

	if expected == nil || actual == nil {
		return expected == actual
	}

	if reflect.DeepEqual(expected, actual) {
		return true
	}

	expectedValue := reflect.ValueOf(expected)
	actualValue := reflect.ValueOf(actual)

	if expectedValue == actualValue {
		return true
	}

	// Attempt comparison after type conversion
	if actualValue.Type().ConvertibleTo(expectedValue.Type()) && expectedValue == actualValue.Convert(expectedValue.Type()) {
		return true
	}

	// Last ditch effort
	if fmt.Sprintf("%#v", expected) == fmt.Sprintf("%#v", actual) {
		return true
	}

	return false
}
