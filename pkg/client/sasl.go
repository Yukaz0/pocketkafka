package client

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// MechanismPlain is the SASL mechanism this client implements. PLAIN is what the
// SCADA clusters in this environment offer; SCRAM would need the client half of
// RFC 5802 as well.
const MechanismPlain = "PLAIN"

// SASL carries client credentials. An empty Mechanism means no authentication,
// which is correct for a broker that does not require it.
type SASL struct {
	Mechanism string
	Username  string
	Password  string
}

// plainToken is the SASL/PLAIN response: authzid NUL authcid NUL passwd.
func plainToken(user, pass string) []byte {
	token := make([]byte, 0, len(user)+len(pass)+2)
	token = append(token, 0)
	token = append(token, user...)
	token = append(token, 0)
	token = append(token, pass...)
	return token
}

// readOnlyAPIKeys are the APIs a monitoring client may use: enough to describe a
// cluster and read offsets, never enough to change anything. Enforcing it at the
// request layer means a read-only client cannot write even if a caller tries.
var readOnlyAPIKeys = map[int16]bool{
	protocol.APKApiVersions:      true,
	protocol.APKSaslHandshake:    true,
	protocol.APKSaslAuthenticate: true,
	protocol.APKMetadata:         true,
	protocol.APKListOffsets:      true,
	protocol.APKFetch:            true,
	protocol.APKListGroups:       true,
	protocol.APKDescribeGroups:   true,
	protocol.APKOffsetFetch:      true,
	protocol.APKFindCoordinator:  true,
}

// ErrReadOnly is returned when a read-only client is asked for an API that can
// change the cluster.
var ErrReadOnly = errors.New("client is read-only: this API can change the cluster")

// checkAPI refuses any API a read-only client must not use, before anything is
// written to a broker.
func (c *KafkaClient) checkAPI(apiKey int16) error {
	if c.readOnly && !readOnlyAPIKeys[apiKey] {
		return fmt.Errorf("%w (api key %d)", ErrReadOnly, apiKey)
	}
	return nil
}

// authenticate runs the SASL exchange. ApiVersions is answered before
// authentication by the brokers this targets, which is why negotiation happens
// first; everything after it is refused until the exchange succeeds.
func (c *KafkaClient) authenticate() error {
	if c.sasl.Mechanism == "" {
		return nil
	}
	if !strings.EqualFold(c.sasl.Mechanism, MechanismPlain) {
		return fmt.Errorf("unsupported SASL mechanism %q (this client implements %s)", c.sasl.Mechanism, MechanismPlain)
	}

	handshake, err := protocol.EncodeSASLHandshakeRequest(&protocol.SASLHandshakeRequest{
		Version:   1,
		Mechanism: MechanismPlain,
	})
	if err != nil {
		return err
	}
	body, err := c.roundTrip(protocol.APKSaslHandshake, 1, handshake)
	if err != nil {
		return fmt.Errorf("sasl handshake: %w", err)
	}
	hsResp, err := protocol.DecodeSASLHandshakeResponse(1, body)
	if err != nil {
		return fmt.Errorf("sasl handshake: %w", err)
	}
	if hsResp.ErrorCode != 0 {
		return fmt.Errorf("sasl handshake rejected: broker error %d (offers %v)", hsResp.ErrorCode, hsResp.Mechanisms)
	}

	token, err := protocol.EncodeSaslAuthenticateRequest(&protocol.SaslAuthenticateRequest{
		Version:   1,
		AuthBytes: plainToken(c.sasl.Username, c.sasl.Password),
	})
	if err != nil {
		return err
	}
	body, err = c.roundTrip(protocol.APKSaslAuthenticate, 1, token)
	if err != nil {
		return fmt.Errorf("sasl authenticate: %w", err)
	}
	authResp, err := protocol.DecodeSaslAuthenticateResponse(1, body)
	if err != nil {
		return fmt.Errorf("sasl authenticate: %w", err)
	}
	if authResp.ErrorCode != 0 {
		// The broker's message is the only clue for a wrong password, and it
		// never echoes the password back.
		msg := "authentication failed"
		if authResp.ErrorMessage != nil && *authResp.ErrorMessage != "" {
			msg = *authResp.ErrorMessage
		}
		return fmt.Errorf("sasl authenticate as %q rejected: broker error %d: %s",
			c.sasl.Username, authResp.ErrorCode, msg)
	}
	return nil
}
