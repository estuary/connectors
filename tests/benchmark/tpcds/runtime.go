package main

import "runtime"

func runtimeCaller() (uintptr, string, int, bool) { return runtime.Caller(0) }
