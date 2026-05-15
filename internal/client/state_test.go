package client

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestClientStateInitial(t *testing.T) {
	s := NewClientState()
	require.Equal(t, TxState(0), s.Tx())
	require.Equal(t, "uninitialized", s.Tx().String())
	require.False(t, s.Tx().IsIdle())
}

func TestClientStateFirstReadyIdle(t *testing.T) {
	s := NewClientState()
	boundary := s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.False(t, boundary, "first idle is not a boundary (was uninitialized)")
	require.Equal(t, TxIdle, s.Tx())
	require.True(t, s.Tx().IsIdle())
}

func TestClientStateBeginCommit(t *testing.T) {
	s := NewClientState()
	_ = s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	// Begin: 'I' -> 'T'.
	boundary := s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	require.True(t, boundary, "idle->in-tx is a boundary")
	require.Equal(t, TxInBlock, s.Tx())
	require.Equal(t, uint64(1), s.TxStarts)

	// Commit: 'T' -> 'I'.
	boundary = s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.True(t, boundary)
	require.Equal(t, TxIdle, s.Tx())
	require.Equal(t, uint64(1), s.TxCommits)
	require.Equal(t, uint64(0), s.TxRollbacks)
}

func TestClientStateBeginErrorRollback(t *testing.T) {
	s := NewClientState()
	_ = s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'T'})

	// Failed: 'T' -> 'E'.
	boundary := s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'E'})
	require.False(t, boundary, "T->E is not a release-boundary (still in tx, just failed)")
	require.Equal(t, TxFailed, s.Tx())

	// Rollback: 'E' -> 'I'.
	boundary = s.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.True(t, boundary)
	require.Equal(t, TxIdle, s.Tx())
	require.Equal(t, uint64(1), s.TxRollbacks)
	require.Equal(t, uint64(0), s.TxCommits)
}

func TestClientStateNonReadyForQueryIgnored(t *testing.T) {
	s := NewClientState()
	boundary := s.ObserveBackendMessage(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	require.False(t, boundary)
	require.Equal(t, TxState(0), s.Tx())
}

func TestClientStateClientMessageCounts(t *testing.T) {
	s := NewClientState()
	s.ObserveClientMessage(&pgproto3.Query{String: "SELECT 1"})
	s.ObserveClientMessage(&pgproto3.Parse{Name: "p", Query: "SELECT 2"})
	s.ObserveClientMessage(&pgproto3.Sync{}) // not counted
	require.Equal(t, uint64(2), s.QueriesIssued)
}
