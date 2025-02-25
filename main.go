package main

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/tidwall/btree"
)

func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func assertEq[C comparable](a, b C, prefix string) {
	if a != b {
		panic(fmt.Sprintf("%s '%v' != '%v'", prefix, a, b))
	}
}

var DEBUG = slices.Contains(os.Args, "--debug")

func debug(a ...any) {
	if !DEBUG {
		return
	}

	a = append([]any{"DEBUG"}, a...)
	fmt.Println(a...)
}

type Value struct {
	txStartId uint64
	txEndId   uint64
	value     string
}

type TransactionState uint8

const (
	TransactionStateInProgress TransactionState = iota
	TransactionStateAborted
	TransactionStateCommitted
)

type IsolationLevel uint8

// Ordered isolation level enum. Stricter isolation levels have a bigger value.
const (
	IsolationLevelReadUncommitted IsolationLevel = iota
	IsolationLevelReadCommitted
	IsolationLevelRepeatableRead
	IsolationLevelSnapshot
	IsolationLevelSerializable
)

const (
	errNoSuchKey          = "no such key"
	errWriteWriteConflict = "write-write conflict"
	errReadWriteConflict  = "read-write conflict"
)

type Transaction struct {
	id        uint64
	isolation IsolationLevel
	state     TransactionState

	// Used by repeatable read isolation or stricter

	// The set of in-progress transactions at the time this transaction is
	// created identified by their keys.
	inprogress btree.Set[uint64]

	// Used by snapshot isolation or stricter

	// The set of values modified by this transaction during its lifetime
	// identified by their keys.
	writeset btree.Set[string]
	// The set of values read by this transaction during its lifetime identified
	// by their keys.
	readset btree.Set[string]
}

type Database struct {
	defaultIsolation  IsolationLevel
	store             map[string][]Value
	transactions      btree.Map[uint64, Transaction]
	nextTransactionId uint64
}

func newDatabase() *Database {
	return &Database{
		defaultIsolation:  IsolationLevelReadCommitted,
		store:             map[string][]Value{},
		nextTransactionId: 1,
	}
}

func (d *Database) inprogress() btree.Set[uint64] {
	ids := btree.Set[uint64]{}
	iter := d.transactions.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if iter.Value().state == TransactionStateInProgress {
			ids.Insert(iter.Key())
		}
	}
	return ids
}

func (d *Database) newTransaction() *Transaction {
	t := Transaction{
		isolation:  d.defaultIsolation,
		state:      TransactionStateInProgress,
		id:         d.nextTransactionId,
		inprogress: d.inprogress(),
	}

	d.nextTransactionId += 1
	d.transactions.Set(t.id, t)

	debug("starting transaction", t.id)

	return &t
}

func setsShareItem(s1, s2 btree.Set[string]) bool {
	s1Iter := s1.Iter()
	s2Iter := s2.Iter()

	for ok := s1Iter.First(); ok; ok = s1Iter.Next() {
		if s2Iter.Seek(s1Iter.Key()) {
			return true
		}
	}

	return false
}

func isWriteWriteConflict(t1, t2 *Transaction) bool {
	return setsShareItem(t1.writeset, t2.writeset)
}

func isReadWriteConflict(t1, t2 *Transaction) bool {
	return setsShareItem(t1.readset, t2.writeset) || setsShareItem(t2.writeset, t1.readset)
}

func (d *Database) completeTransaction(t *Transaction, state TransactionState) error {
	debug("completing transaction ", t.id)

	d.assertValidTransaction(t)
	assert(state != TransactionStateInProgress, "not InProgress state")

	if state == TransactionStateCommitted {
		if t.isolation == IsolationLevelSnapshot && d.hasConflict(t, isWriteWriteConflict) {
			d.completeTransaction(t, TransactionStateAborted)
			return errors.New(errWriteWriteConflict)
		}

		if t.isolation == IsolationLevelSerializable && d.hasConflict(t, isReadWriteConflict) {
			d.completeTransaction(t, TransactionStateAborted)
			return errors.New(errReadWriteConflict)
		}
	}

	t.state = state
	d.transactions.Set(t.id, *t)

	return nil
}

func (d *Database) assertValidTransaction(t *Transaction) {
	assert(t.id > 0, "valid transaction id")
	assert(t.state == TransactionStateInProgress, "transaction in progress")
}

func (d *Database) transaction(id uint64) Transaction {
	tx, ok := d.transactions.Get(id)
	assert(ok, "valid transaction")
	return tx
}

func (d *Database) isVisible(t *Transaction, value Value) bool {
	// Refer to the 1999 ANSI SQL standard (page 84) for the meaning of each isolation level.

	if t.isolation == IsolationLevelReadUncommitted {
		// All values are visible even if not committed, we merely verify that
		// the value has not been deleted.
		return value.txEndId == 0
	}

	if t.isolation == IsolationLevelReadCommitted {
		// Started by another transaction but it's not committed.
		if value.txStartId != t.id && d.transaction(value.txStartId).state != TransactionStateCommitted {
			return false
		}

		// Deleted by this transaction so should not be visible anymore.
		if value.txEndId == t.id {
			return false
		}

		// Deleted by another committed transaction.
		if value.txEndId > 0 && d.transaction(value.txEndId).state == TransactionStateCommitted {
			return false
		}

		// Otherwise visible.
		return true
	}

	// Repeatable read and stricter
	assert(t.isolation >= IsolationLevelRepeatableRead, "repeatable read or stricter")

	// Started after this transaction.
	if value.txStartId > t.id {
		return false
	}

	// Started by transactions in progress.
	if t.inprogress.Contains(value.txStartId) {
		return false
	}

	// Started by other transactions that are not committed yet.
	if value.txStartId != t.id && d.transaction(value.txStartId).state != TransactionStateCommitted {
		return false
	}

	// Value was deleted in other committed transaction that started before this one
	if value.txEndId > 0 && value.txEndId < t.id &&
		!t.inprogress.Contains(value.txEndId) &&
		d.transaction(value.txEndId).state == TransactionStateCommitted {
		return false
	}

	return true
}

func (d *Database) hasConflict(t1 *Transaction, conflictFn func(*Transaction, *Transaction) bool) bool {
	iter := d.transactions.Iter()
	inprogressIter := t1.inprogress.Iter()
	for ok := inprogressIter.First(); ok; ok = inprogressIter.Next() {
		if !iter.Seek(inprogressIter.Key()) {
			continue
		}

		t2 := iter.Value()
		if t2.state == TransactionStateCommitted && conflictFn(t1, &t2) {
			return true
		}
	}

	for id := t1.id; id < d.nextTransactionId; id++ {
		if !iter.Seek(id) {
			continue
		}

		t2 := iter.Value()
		if t2.state == TransactionStateCommitted && conflictFn(t1, &t2) {
			return true
		}
	}

	return false
}

type Connection struct {
	tx *Transaction
	db *Database
}

func (c *Connection) execCommand(command string, args []string) (string, error) {
	debug(command, args)

	if command == "begin" {
		assertEq(c.tx, nil, "no running transaction")
		c.tx = c.db.newTransaction()
		return fmt.Sprintf("%d", c.tx.id), nil
	}

	if command == "abort" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, TransactionStateAborted)
		c.tx = nil
		return "", err
	}

	if command == "commit" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, TransactionStateCommitted)
		c.tx = nil
		return "", err
	}

	if command == "get" {
		c.db.assertValidTransaction(c.tx)
		key := args[0]
		c.tx.readset.Insert(key)
		for i := len(c.db.store[key]) - 1; i >= 0; i -= 1 {
			value := c.db.store[key][i]
			debug(value, c.tx, c.db.isVisible(c.tx, value))
			if c.db.isVisible(c.tx, value) {
				return value.value, nil
			}
		}

		return "", errors.New(errNoSuchKey)
	}

	if command == "set" || command == "delete" {
		c.db.assertValidTransaction(c.tx)
		key := args[0]

		found := false
		for i := len(c.db.store[key]) - 1; i >= 0; i -= 1 {
			value := &c.db.store[key][i]
			debug(value, c.tx, c.db.isVisible(c.tx, *value))
			if c.db.isVisible(c.tx, *value) {
				value.txEndId = c.tx.id
				found = true
			}
		}

		if command == "delete" && !found {
			return "", errors.New(errNoSuchKey)
		}

		c.tx.writeset.Insert(key)

		if command == "set" {
			value := args[1]
			c.db.store[key] = append(c.db.store[key], Value{
				txStartId: c.tx.id,
				txEndId:   0,
				value:     value,
			})

			return value, nil
		}

		// Delete ok.
		return "", nil
	}

	return "", errors.New("unimplemented")
}

func (c *Connection) mustExecCommand(cmd string, args []string) string {
	res, err := c.execCommand(cmd, args)
	assertEq(err, nil, "unexpected error")
	return res
}

func (d *Database) newConnection() *Connection {
	return &Connection{
		tx: nil,
		db: d,
	}
}

func main() {
	panic("unimplemented")
}
