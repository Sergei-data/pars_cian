package pipeline

import (
	"sync/atomic"
)

type Command interface {
	// Called when the process should terminate.
	// It cannot be called when interruption is disallowed.
	// Everything, up to the last AllowInterruption(true) call, *happened before*.
	Interrupt()
	// Starts the process and waits for it to finish.
	Run(ExecutionContext)
}

type ExecutionContext interface {
	// Returns true if the process should be interrupted.
	// It will be interrupted when possible (or never if at the end of the process interruption is still disallowed).
	ShouldInterrupt() bool
	// Allows or disallows interruption.
	// It returns true if flag changed, false if the interruption happened or the flag didn't change.
	// By default, interruption is disallowed.
	AllowInterruption(canBeInterrupted bool) (changed bool)
}

// Can be started and stopped concurrently.
type AtomicProcess[T Command] struct {
	command      T
	state        atomic.Int32
	canInterrupt chan bool
	recover      chan bool
}

// Process state
const (
	_Stopped     int32 = 0
	_Running     int32 = 1
	_Interrupted int32 = 2
)

func NewAtomicProcess[T Command](command T) *AtomicProcess[T] {
	return &AtomicProcess[T]{
		command:      command,
		canInterrupt: make(chan bool, 1),
		recover:      make(chan bool, 1),
	}
}

// Returns true if the process is sent to be interrupted,
// false if it's not running (or not sent to start) or already is sent to be interrupted.
func (p *AtomicProcess[T]) TryStop() bool {
	if !p.state.CompareAndSwap(_Running, _Interrupted) {
		return false
	}

	go func() {
		defer func() {
			p.canInterrupt <- true
			p.recover <- true
		}()
		if <-p.canInterrupt {
			p.command.Interrupt()
		}
	}()

	return true
}

// Returns true if the process is sent to start,
// false if it's already running or sent to start.
func (p *AtomicProcess[T]) TryStart() bool {
	if !p.state.CompareAndSwap(_Stopped, _Running) {
		return false
	}

	go func() {
		defer func() {
			if p.state.CompareAndSwap(_Running, _Stopped) {
				p.AllowInterruption(false)
				return
			}
			select {
			// try to reject interruption
			case p.canInterrupt <- false: // interruption is happening right now or disallowed
			default: // interruption is already happened or hasn't started yet
			}
			<-p.recover
			<-p.canInterrupt
			p.state.Store(_Stopped)
		}()
		p.command.Run(p)
	}()

	return true
}

// See ExecutionContext. This method is useless outside of Command.Run()
func (p *AtomicProcess[T]) ShouldInterrupt() bool {
	return p.state.Load() == _Interrupted
}

// See ExecutionContext. This method is useless outside of Command.Run()
func (p *AtomicProcess[T]) AllowInterruption(canBeInterrupted bool) (changed bool) {
	if canBeInterrupted {
		select {
		case p.canInterrupt <- true:
			return true
		default:
			return false // flag didn't change
		}
	}
	select {
	case <-p.canInterrupt:
		return true
	default:
		return false // interruption happened or flag didn't change
	}
}
