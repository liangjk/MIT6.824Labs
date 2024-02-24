package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Assert(condition bool, message string, a ...interface{}) {
	if Debug && !condition {
		DPrintf(message, a...)
	}
}
