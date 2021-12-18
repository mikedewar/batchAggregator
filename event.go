package main

import "encoding"

type Event interface {
	encoding.BinaryUnmarshaler
	encoding.BinaryMarshaler
}
