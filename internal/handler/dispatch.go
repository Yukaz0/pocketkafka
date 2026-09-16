package handler

import (
	"fmt"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// This file holds the request dispatcher: the ApiVersions table the broker
// advertises, the top-level routing switch, and the fail-closed handling of
// APIs the broker does not implement. The per-API handlers live in handler.go.

// supportedKeys returns the ApiVersions table advertised by this broker.
func (h *Handler) supportedKeys() []protocol.ApiKeySupport {
	return []protocol.ApiKeySupport{
		{ApiKey: protocol.APKProduce, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKProduce)},
		{ApiKey: protocol.APKFetch, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKFetch)},
		{ApiKey: protocol.APKListOffsets, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKListOffsets)},
		{ApiKey: protocol.APKMetadata, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKMetadata)},
		{ApiKey: protocol.APKOffsetCommit, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKOffsetCommit)},
		{ApiKey: protocol.APKOffsetFetch, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKOffsetFetch)},
		{ApiKey: protocol.APKFindCoordinator, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKFindCoordinator)},
		{ApiKey: protocol.APKJoinGroup, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKJoinGroup)},
		{ApiKey: protocol.APKHeartbeat, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKHeartbeat)},
		{ApiKey: protocol.APKLeaveGroup, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKLeaveGroup)},
		{ApiKey: protocol.APKSyncGroup, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSyncGroup)},
		{ApiKey: protocol.APKDescribeGroups, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDescribeGroups)},
		{ApiKey: protocol.APKListGroups, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKListGroups)},
		{ApiKey: protocol.APKSaslHandshake, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSaslHandshake)},
		{ApiKey: protocol.APKApiVersions, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKApiVersions)},
		{ApiKey: protocol.APKCreateTopics, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKCreateTopics)},
		{ApiKey: protocol.APKDeleteTopics, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDeleteTopics)},
		{ApiKey: protocol.APKInitProducerID, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKInitProducerID)},
		{ApiKey: protocol.APKSaslAuthenticate, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSaslAuthenticate)},
		{ApiKey: protocol.APKDeleteGroups, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDeleteGroups)},
	}
}

// SetAdvertisedForPort registers the advertised address for connections
// accepted through the listener bound to bindPort.
func (h *Handler) SetAdvertisedForPort(bindPort int32, host string, port int32) {
	if h.advertisedByBind == nil {
		h.advertisedByBind = make(map[int32]listenerAdvertised)
	}
	h.advertisedByBind[bindPort] = listenerAdvertised{host: host, port: port}
}

// advertisedFor resolves the advertised address for a connection accepted
// through bindPort, falling back to the primary advertised address.
func (h *Handler) advertisedFor(bindPort int32) (string, int32) {
	if a, ok := h.advertisedByBind[bindPort]; ok {
		return a.host, a.port
	}
	return h.advertisedHost, h.advertisedPort
}

// Handle decodes and processes a single request body, returning the encoded
// response body (the correlation ID is added by the server layer). ctx carries
// the authenticated principal used for authorization.
func (h *Handler) Handle(apiKey, version int16, body []byte, ctx RequestContext) ([]byte, error) {
	maxV := protocol.MaxVersion(apiKey)
	if apiKey != protocol.APKApiVersions && (maxV < 0 || version > maxV || version < 0) {
		return h.unsupportedVersion(apiKey, version, body)
	}

	switch apiKey {
	case protocol.APKApiVersions:
		return h.handleApiVersions(version, body)
	case protocol.APKMetadata:
		return h.handleMetadata(version, body, ctx.ListenerPort)
	case protocol.APKProduce:
		return h.handleProduce(version, body, ctx)
	case protocol.APKFetch:
		return h.handleFetch(version, body, ctx)
	case protocol.APKListOffsets:
		return h.handleListOffsets(version, body, ctx)
	case protocol.APKCreateTopics:
		return h.handleCreateTopics(version, body, ctx)
	case protocol.APKDeleteTopics:
		return h.handleDeleteTopics(version, body, ctx)
	case protocol.APKFindCoordinator:
		return h.handleFindCoordinator(version, body, ctx.ListenerPort)
	case protocol.APKJoinGroup:
		return h.handleJoinGroup(version, body, ctx)
	case protocol.APKSyncGroup:
		return h.handleSyncGroup(version, body, ctx)
	case protocol.APKHeartbeat:
		return h.handleHeartbeat(version, body, ctx)
	case protocol.APKLeaveGroup:
		return h.handleLeaveGroup(version, body, ctx)
	case protocol.APKOffsetCommit:
		return h.handleOffsetCommit(version, body, ctx)
	case protocol.APKOffsetFetch:
		return h.handleOffsetFetch(version, body, ctx)
	case protocol.APKListGroups:
		return h.handleListGroups(version, body)
	case protocol.APKDescribeGroups:
		return h.handleDescribeGroups(version, body)
	case protocol.APKDeleteGroups:
		return h.handleDeleteGroups(version, body, ctx)
	case protocol.APKInitProducerID:
		return h.handleInitProducerID(version, body)
	default:
		return nil, fmt.Errorf("unsupported api key %d", apiKey)
	}
}

// disabledTxnMaxVersion is the last wire version whose request/response codecs
// we are prepared to run for the transactional APIs. The broker does not
// implement transactions, so these requests only ever receive
// UNSUPPORTED_VERSION; the cap keeps us from mis-encoding a version we do not
// actually understand.
var disabledTxnMaxVersion = map[int16]int16{
	protocol.APKAddPartitionsToTxn: 1,
	protocol.APKAddOffsetsToTxn:    1,
	protocol.APKEndTxn:             2,
}

// unsupportedVersion builds a response body reporting UNSUPPORTED_VERSION. For
// ApiVersions we include the supported keys so the client can downgrade. For
// the transactional APIs (whose codecs exist but whose semantics do not) we
// answer with a well-formed UNSUPPORTED_VERSION response instead of a success,
// so a client never mistakes a no-op for a committed transaction.
func (h *Handler) unsupportedVersion(apiKey, version int16, body []byte) ([]byte, error) {
	if apiKey == protocol.APKApiVersions {
		resp := &protocol.ApiVersionsResponse{
			Version:   protocol.MaxVersion(protocol.APKApiVersions),
			ErrorCode: protocol.ErrUnsupportedVersion,
			ApiKeys:   h.supportedKeys(),
		}
		return protocol.EncodeApiVersionsResponse(resp)
	}
	if maxDecodable, ok := disabledTxnMaxVersion[apiKey]; ok && version >= 0 && version <= maxDecodable {
		return disabledTransactionResponse(apiKey, version, body)
	}
	return nil, fmt.Errorf("unsupported version %d for api key %d", version, apiKey)
}

// disabledTransactionResponse decodes a transactional request solely to learn
// its version, then replies UNSUPPORTED_VERSION using the matching response
// codec. No transaction state is created.
func disabledTransactionResponse(apiKey, version int16, body []byte) ([]byte, error) {
	switch apiKey {
	case protocol.APKAddPartitionsToTxn:
		if _, err := protocol.DecodeAddPartitionsToTxnRequest(version, body); err != nil {
			return nil, err
		}
		return protocol.EncodeAddPartitionsToTxnResponse(&protocol.AddPartitionsToTxnResponse{
			Version:   version,
			ErrorCode: protocol.ErrUnsupportedVersion,
		})
	case protocol.APKAddOffsetsToTxn:
		if _, err := protocol.DecodeAddOffsetsToTxnRequest(version, body); err != nil {
			return nil, err
		}
		return protocol.EncodeAddOffsetsToTxnResponse(&protocol.AddOffsetsToTxnResponse{
			Version:   version,
			ErrorCode: protocol.ErrUnsupportedVersion,
		})
	case protocol.APKEndTxn:
		if _, err := protocol.DecodeEndTxnRequest(version, body); err != nil {
			return nil, err
		}
		return protocol.EncodeEndTxnResponse(&protocol.EndTxnResponse{
			Version:   version,
			ErrorCode: protocol.ErrUnsupportedVersion,
		})
	}
	return nil, fmt.Errorf("unsupported api key %d", apiKey)
}
