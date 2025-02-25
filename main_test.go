package main

import (
	"testing"
)

func TestReadUncommitted(t *testing.T) {
	db := newDatabase()
	db.defaultIsolation = IsolationLevelReadUncommitted

	c1 := db.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := db.newConnection()
	c2.mustExecCommand("begin", nil)

	c1.mustExecCommand("set", []string{"x", "c1"})

	// c1 update is visible to itself
	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "c1", "c1 get x")

	// c1 update is also visible to other transactions
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "c1", "c2 get x")

	// And if we delete that should be visible to all transactions as well
	_ = c1.mustExecCommand("delete", []string{"x"})

	res, err := c1.execCommand("get", []string{"x"})
	assertEq(res, "", "c1 sees no x")
	assertEq(err.Error(), errNoSuchKey, "c1 sees no x")

	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 sees no x")
	assertEq(err.Error(), errNoSuchKey, "c2 sees no x")
}

func TestReadCommitted(t *testing.T) {
	db := newDatabase()
	db.defaultIsolation = IsolationLevelReadCommitted

	c1 := db.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := db.newConnection()
	c2.mustExecCommand("begin", nil)

	// Local change is visible locally.
	c1.mustExecCommand("set", []string{"x", "hey"})

	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// Update not available to this transaction since this is not
	// committed.
	res, err := c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), errNoSuchKey, "c2 get x")

	c1.mustExecCommand("commit", nil)

	// Now that it's been committed, it's visible in c2.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	c3 := db.newConnection()
	c3.mustExecCommand("begin", nil)

	// Local change is visible locally.
	c3.mustExecCommand("set", []string{"x", "yall"})

	res = c3.mustExecCommand("get", []string{"x"})
	assertEq(res, "yall", "c3 get x")

	// But not on the other commit, again.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	c3.mustExecCommand("abort", nil)

	// And still not, if the other transaction aborted.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	// And if we delete it, it should show up deleted locally.
	c2.mustExecCommand("delete", []string{"x"})

	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), errNoSuchKey, "c2 get x")

	c2.mustExecCommand("commit", nil)

	// It should also show up as deleted in new transactions now
	// that it has been committed.
	c4 := db.newConnection()
	c4.mustExecCommand("begin", nil)

	res, err = c4.execCommand("get", []string{"x"})
	assertEq(res, "", "c4 get x")
	assertEq(err.Error(), errNoSuchKey, "c4 get x")
}

func TestRepeatableRead(t *testing.T) {
	db := newDatabase()
	db.defaultIsolation = IsolationLevelRepeatableRead

	c1 := db.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := db.newConnection()
	c2.mustExecCommand("begin", nil)

	// Local change is visible locally.
	c1.mustExecCommand("set", []string{"x", "hey"})
	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// Update not available to this transaction since this is not
	// committed.
	res, err := c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), errNoSuchKey, "c2 get x")

	c1.mustExecCommand("commit", nil)

	// Even after committing, it's not visible in an existing
	// transaction.
	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), errNoSuchKey, "c2 get x")

	// But is available in a new transaction.
	c3 := db.newConnection()
	c3.mustExecCommand("begin", nil)

	res = c3.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c3 get x")

	// Local change is visible locally.
	c3.mustExecCommand("set", []string{"x", "yall"})
	res = c3.mustExecCommand("get", []string{"x"})
	assertEq(res, "yall", "c3 get x")

	// But not on the other commit, again.
	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), errNoSuchKey, "c2 get x")

	c3.mustExecCommand("abort", nil)

	// And still not, regardless of abort, because it's an older
	// transaction.
	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), errNoSuchKey, "c2 get x")

	// And again still the aborted set is still not on a new
	// transaction.
	c4 := db.newConnection()
	c4.mustExecCommand("begin", nil)

	res = c4.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c4 get x")

	c4.mustExecCommand("delete", []string{"x"})
	c4.mustExecCommand("commit", nil)

	// But the delete is visible to new transactions now that this
	// has been committed.
	c5 := db.newConnection()
	c5.mustExecCommand("begin", nil)

	res, err = c5.execCommand("get", []string{"x"})
	assertEq(res, "", "c5 get x")
	assertEq(err.Error(), errNoSuchKey, "c5 get x")
}

func TestSnapshotIsolation_writewrite_conflict(t *testing.T) {
	db := newDatabase()
	db.defaultIsolation = IsolationLevelSnapshot

	c1 := db.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := db.newConnection()
	c2.mustExecCommand("begin", nil)

	c3 := db.newConnection()
	c3.mustExecCommand("begin", nil)

	c1.mustExecCommand("set", []string{"x", "hey"})
	c1.mustExecCommand("commit", nil)

	c2.mustExecCommand("set", []string{"x", "hey"})

	res, err := c2.execCommand("commit", nil)
	assertEq(res, "", "c2 commit")
	assertEq(err.Error(), errWriteWriteConflict, "c2 commit")

	// But unrelated keys cause no conflict.
	c3.mustExecCommand("set", []string{"y", "no conflict"})
	c3.mustExecCommand("commit", nil)
}

func TestSerializableIsolation_readwrite_conflict(t *testing.T) {
	db := newDatabase()
	db.defaultIsolation = IsolationLevelSerializable

	c1 := db.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := db.newConnection()
	c2.mustExecCommand("begin", nil)

	c3 := db.newConnection()
	c3.mustExecCommand("begin", nil)

	c1.mustExecCommand("set", []string{"x", "hey"})
	c1.mustExecCommand("commit", nil)

	_, err := c2.execCommand("get", []string{"x"})
	assertEq(err.Error(), errNoSuchKey, "c5 get x")

	res, err := c2.execCommand("commit", nil)
	assertEq(res, "", "c2 commit")
	assertEq(err.Error(), errReadWriteConflict, "c2 commit")

	// But unrelated keys cause no conflict.
	c3.mustExecCommand("set", []string{"y", "no conflict"})
	c3.mustExecCommand("commit", nil)
}
