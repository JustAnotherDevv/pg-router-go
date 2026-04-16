package proto

// MsgType is the one-byte tag that prefixes most pgwire v3 messages
// (except StartupMessage / SSLRequest / GSSEncRequest / CancelRequest,
// which are length-prefixed only).
//
// Source of truth: PostgreSQL docs, "Message Formats" section.
// https://www.postgresql.org/docs/current/protocol-message-formats.html
type MsgType byte

// Frontend message tags (client → server).
const (
	TypeQuery               MsgType = 'Q'
	TypeParse               MsgType = 'P'
	TypeBind                MsgType = 'B'
	TypeExecute             MsgType = 'E' // ambiguous with ErrorResponse; see Direction
	TypeDescribe            MsgType = 'D' // ambiguous with DataRow
	TypeClose               MsgType = 'C' // ambiguous with CommandComplete + Close (backend)
	TypeFlush               MsgType = 'H'
	TypeSync                MsgType = 'S' // ambiguous with ParameterStatus + Sync
	TypeTerminate           MsgType = 'X'
	TypePasswordMessage     MsgType = 'p' // also SASLInitialResponse, SASLResponse, GSSResponse
	TypeFunctionCall        MsgType = 'F'
	TypeCopyData            MsgType = 'd' // ambiguous; both directions
	TypeCopyDone            MsgType = 'c'
	TypeCopyFail            MsgType = 'f'
)

// Backend message tags (server → client). Some collide with frontend tags;
// disambiguate via Direction.
const (
	TypeAuthentication     MsgType = 'R' // covers OK/MD5/SASL/SASLContinue/SASLFinal
	TypeParameterStatus    MsgType = 'S'
	TypeBackendKeyData     MsgType = 'K'
	TypeReadyForQuery      MsgType = 'Z'
	TypeRowDescription     MsgType = 'T'
	TypeDataRow            MsgType = 'D'
	TypeCommandComplete    MsgType = 'C'
	TypeParseComplete      MsgType = '1'
	TypeBindComplete       MsgType = '2'
	TypeCloseComplete      MsgType = '3'
	TypeNoData             MsgType = 'n'
	TypeParameterDesc      MsgType = 't'
	TypeEmptyQueryResponse MsgType = 'I'
	TypePortalSuspended    MsgType = 's'
	TypeErrorResponse      MsgType = 'E'
	TypeNoticeResponse     MsgType = 'N'
	TypeNotification       MsgType = 'A'
	TypeCopyInResponse     MsgType = 'G'
	TypeCopyOutResponse    MsgType = 'H' // collides with Flush; direction disambiguates
	TypeCopyBothResponse   MsgType = 'W'
	TypeNegotiateProtocol  MsgType = 'v'
)

// Direction labels which way a message flows through pgrouter.
type Direction uint8

const (
	// ToServer = frontend message originating at the client, heading to
	// the upstream Postgres.
	ToServer Direction = iota
	// ToClient = backend message originating at the upstream Postgres,
	// heading to the client.
	ToClient
)

// String returns the conventional name for the direction.
func (d Direction) String() string {
	switch d {
	case ToServer:
		return "client→server"
	case ToClient:
		return "server→client"
	default:
		return "unknown"
	}
}
