package agent

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

// =============================================================================
// Linux pseudo-terminal helpers
// =============================================================================
//
// Just enough of a PTY layer for the web-terminal feature, written natively
// against the unix package so we don't have to pull in github.com/creack/pty
// for ~40 lines of glue.
//
// Linux-only. We don't run on anything else, but if that ever changes
// the constants here (TIOCSPTLCK, TIOCGPTN, /dev/pts/N path) need a port.

// openPTY opens a new pseudo-terminal pair via /dev/ptmx. Returns the
// master file and the path to the slave (e.g. "/dev/pts/3"). Caller is
// responsible for closing master; the slave is opened by startPTY.
func openPTY() (master *os.File, slaveName string, err error) {
	master, err = os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, "", fmt.Errorf("open /dev/ptmx: %w", err)
	}
	// Unlock the slave end so we can open it.
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		master.Close()
		return nil, "", fmt.Errorf("ioctl TIOCSPTLCK: %w", err)
	}
	// Read the slave's number.
	n, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		master.Close()
		return nil, "", fmt.Errorf("ioctl TIOCGPTN: %w", err)
	}
	return master, fmt.Sprintf("/dev/pts/%d", n), nil
}

// startPTY starts cmd with its stdin/stdout/stderr wired to a new
// pseudo-terminal, sets it as the controlling terminal, and returns the
// master end. Caller is responsible for cmd.Wait() and closing the master.
//
// Sets a default 80x24 winsize; the WebSocket caller will resize to match
// the user's actual terminal as soon as xterm.js connects.
func startPTY(cmd *exec.Cmd) (*os.File, error) {
	master, slaveName, err := openPTY()
	if err != nil {
		return nil, err
	}

	slave, err := os.OpenFile(slaveName, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		master.Close()
		return nil, fmt.Errorf("open slave %s: %w", slaveName, err)
	}

	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// Setsid: detach from our session so the child gets its own.
	// Setctty/Ctty=0 (= the slave, which is now stdin/fd 0) makes the
	// slave the child's controlling terminal. Without these, terminal
	// signals like Ctrl-C don't reach the process group correctly.
	cmd.SysProcAttr.Setsid = true
	cmd.SysProcAttr.Setctty = true

	// Default 80x24; xterm.js sends a resize on attach so this is short-lived.
	if err := unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: 24, Col: 80}); err != nil {
		master.Close()
		slave.Close()
		return nil, fmt.Errorf("set initial winsize: %w", err)
	}

	if err := cmd.Start(); err != nil {
		master.Close()
		slave.Close()
		return nil, fmt.Errorf("start cmd: %w", err)
	}
	// Slave fd is now duped into the child; we don't need it locally.
	slave.Close()
	return master, nil
}

// resizePTY changes the pty's reported window size. xterm.js sends this
// on every browser-side resize event so the running program sees the
// correct $COLUMNS/$LINES.
func resizePTY(master *os.File, cols, rows uint16) error {
	return unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{
		Row: rows,
		Col: cols,
	})
}
