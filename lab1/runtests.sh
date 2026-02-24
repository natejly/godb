#!/bin/bash

# Run all Lab 1 tests
echo "Running Bitmap tests..."
go test -v ./storage -run Bitmap
echo "Running HeapPage tests..."
go test -v ./storage -run HeapPage
echo "Running BufferPool tests..."
go test -v ./storage -run BufferPool
echo "Running TableHeap tests..."
go test -v ./execution -run TableHeap
