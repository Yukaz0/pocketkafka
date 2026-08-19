// Package handler implements the per-API-request handlers for the broker,
// decoding requests with pkg/protocol and producing responses backed by the
// storage engine and the group coordinator.
package handler

import (
	"fmt"
	"strings"
	"time"

	"github.com/neu/go-kafka-neu/internal/config"
	"github.com/neu/go-kafka-neu/internal/coordinator"
	"github.com/neu/go-kafka-neu/internal/storage"
	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// Handler routes API requests to the correct logic and encodes responses.
type Handler struct {
	store          *storage.Store
	coord          *coordinator.GroupManager
	cfg            *config.Config
	nodeID         int32
	advertisedHost string
	advertisedPort int32
}

// New builds a Handler bound to the given storage store and group coordinator.
func New(store *storage.Store, coord *coordinator.GroupManager, cfg *config.Config, nodeID int32, host string, port int32) *Handler {
	return &Handler{
		store:          store,
		coord:          coord,
		cfg:            cfg,
		nodeID:         nodeID,
		advertisedHost: host,
		advertisedPort: port,
	}
}

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
		{ApiKey: protocol.APKSaslHandshake, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSaslHandshake)},
		{ApiKey: protocol.APKApiVersions, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKApiVersions)},
		{ApiKey: protocol.APKCreateTopics, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKCreateTopics)},
		{ApiKey: protocol.APKDeleteTopics, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDeleteTopics)},
		{ApiKey: protocol.APKSaslAuthenticate, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSaslAuthenticate)},
	}
}

// Handle decodes and processes a single request body, returning the encoded
// response body (the correlation ID is added by the server layer).
func (h *Handler) Handle(apiKey, version int16, body []byte) ([]byte, error) {
	maxV := protocol.MaxVersion(apiKey)
	if apiKey != protocol.APKApiVersions && (maxV < 0 || version > maxV || version < 0) {
		return h.unsupportedVersion(apiKey, version)
	}

	switch apiKey {
	case protocol.APKApiVersions:
		return h.handleApiVersions(version, body)
	case protocol.APKMetadata:
		return h.handleMetadata(version, body)
	case protocol.APKProduce:
		return h.handleProduce(version, body)
	case protocol.APKFetch:
		return h.handleFetch(version, body)
	case protocol.APKListOffsets:
		return h.handleListOffsets(version, body)
	case protocol.APKCreateTopics:
		return h.handleCreateTopics(version, body)
	case protocol.APKDeleteTopics:
		return h.handleDeleteTopics(version, body)
	case protocol.APKFindCoordinator:
		return h.handleFindCoordinator(version, body)
	case protocol.APKJoinGroup:
		return h.handleJoinGroup(version, body)
	case protocol.APKSyncGroup:
		return h.handleSyncGroup(version, body)
	case protocol.APKHeartbeat:
		return h.handleHeartbeat(version, body)
	case protocol.APKLeaveGroup:
		return h.handleLeaveGroup(version, body)
	case protocol.APKOffsetCommit:
		return h.handleOffsetCommit(version, body)
	case protocol.APKOffsetFetch:
		return h.handleOffsetFetch(version, body)
	default:
		return nil, fmt.Errorf("unsupported api key %d", apiKey)
	}
}

func (h *Handler) handleApiVersions(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeApiVersionsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.ApiVersionsResponse{
		Version:   req.Version,
		ApiKeys:   h.supportedKeys(),
		ErrorCode: protocol.ErrNone,
	}
	if version > protocol.MaxVersion(protocol.APKApiVersions) {
		// Per KIP-511 a broker that does not support the requested ApiVersions
		// version replies with UNSUPPORTED_VERSION and the supported keys
		// encoded in the non-flexible version 0 format, so the client can
		// downgrade.
		resp.ErrorCode = protocol.ErrUnsupportedVersion
		resp.Version = 0
	}
	return protocol.EncodeApiVersionsResponse(resp)
}

func (h *Handler) handleMetadata(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeMetadataRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.MetadataResponse{
		Version:                     version,
		ThrottleTimeMs:              0,
		ClusterAuthorizedOperations: -2147483648,
		Brokers: []protocol.MetadataBroker{{
			NodeID: h.nodeID,
			Host:   h.advertisedHost,
			Port:   h.advertisedPort,
		}},
		ClusterID:    &h.cfg.Broker.ClusterID,
		ControllerID: h.nodeID,
	}

	var requested []string
	if req.Topics != nil {
		for _, t := range req.Topics {
			requested = append(requested, t.Topic)
		}
	} else {
		requested = h.store.TopicNames()
	}

	for _, name := range requested {
		mt := protocol.MetadataTopic{Name: name, IsInternal: false}
		t := h.store.GetTopic(name)
		if t == nil {
			// Auto-create a referenced topic when auto creation is enabled,
			// regardless of the request flag, for robust client interop.
			if h.cfg.Topics.AutoCreate {
				h.store.EnsureTopic(name, h.cfg.Topics.DefaultPartitions)
				t = h.store.GetTopic(name)
			}
			if t == nil {
				mt.ErrorCode = protocol.ErrUnknownTopicOrPartition
				resp.Topics = append(resp.Topics, mt)
				continue
			}
		}
		ids := make([]int, 0, len(t.Partitions))
		for id := range t.Partitions {
			ids = append(ids, int(id))
		}
		for i := 0; i < len(t.Partitions); i++ {
			mt.Partitions = append(mt.Partitions, protocol.MetadataPartition{
				Partition:   int32(i),
				Leader:      h.nodeID,
				LeaderEpoch: 0,
				Replicas:    []int32{h.nodeID},
				ISR:         []int32{h.nodeID},
			})
		}
		_ = ids
		resp.Topics = append(resp.Topics, mt)
	}
	return protocol.EncodeMetadataResponse(resp)
}

func (h *Handler) handleProduce(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeProduceRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.ProduceResponse{Version: version, ThrottleTimeMs: 0}
	now := time.Now().UnixMilli()

	for _, t := range req.Topics {
		rt := protocol.ProduceResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rp := protocol.ProduceResponsePartition{
				Partition:     p.Partition,
				ErrorCode:     protocol.ErrNone,
				LogAppendTime: now,
			}
			part := h.store.GetPartition(t.Topic, p.Partition)
			if part == nil {
				if h.cfg.Topics.AutoCreate {
					h.store.EnsureTopic(t.Topic, h.cfg.Topics.DefaultPartitions)
					part = h.store.GetPartition(t.Topic, p.Partition)
				}
			}
			if part == nil {
				rp.ErrorCode = protocol.ErrUnknownTopicOrPartition
				rp.BaseOffset = -1
			} else {
				base, aerr := part.Append(p.Records)
				if aerr != nil {
					rp.ErrorCode = protocol.ErrCorruptMessage
					rp.BaseOffset = -1
				} else {
					rp.BaseOffset = base
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeProduceResponse(resp)
}

func (h *Handler) handleFetch(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeFetchRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.FetchResponse{Version: version, ThrottleTimeMs: 0}

	for _, t := range req.Topics {
		rt := protocol.FetchResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rp := protocol.FetchResponsePartition{
				Partition:        p.Partition,
				HighWatermark:    0,
				LastStableOffset: -1,
				LogStartOffset:   0,
			}
			part := h.store.GetPartition(t.Topic, p.Partition)
			if part == nil {
				rp.ErrorCode = protocol.ErrUnknownTopicOrPartition
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			h.longPoll(part, p.FetchOffset, req.MaxWaitMillis)
			data, hwm, err := part.Read(p.FetchOffset, p.PartitionMaxBytes)
			rp.HighWatermark = hwm
			rp.LastStableOffset = hwm
			rp.LogStartOffset = part.EarliestOffset()
			if err != nil {
				rp.ErrorCode = protocol.ErrOffsetOutOfRange
			} else {
				rp.Records = data
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeFetchResponse(resp)
}

func (h *Handler) handleListOffsets(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeListOffsetsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.ListOffsetsResponse{Version: version, ThrottleTimeMs: 0}
	for _, t := range req.Topics {
		rt := protocol.ListOffsetsResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rp := protocol.ListOffsetsResponsePartition{
				Partition:   p.Partition,
				ErrorCode:   protocol.ErrNone,
				Timestamp:   -1,
				Offset:      -1,
				LeaderEpoch: -1,
			}
			part := h.store.GetPartition(t.Topic, p.Partition)
			if part == nil {
				rp.ErrorCode = protocol.ErrUnknownTopicOrPartition
			} else {
				off, err := part.GetOffset(p.Timestamp)
				if err != nil {
					rp.ErrorCode = protocol.ErrUnknownServerError
				} else {
					rp.Offset = off
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeListOffsetsResponse(resp)
}

func (h *Handler) handleCreateTopics(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeCreateTopicsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.CreateTopicsResponse{Version: version, ThrottleTimeMs: 0}
	for _, t := range req.Topics {
		ct := protocol.CreateTopicsResponseTopic{Topic: t.Topic, ErrorCode: protocol.ErrNone}
		if req.ValidateOnly {
			resp.Topics = append(resp.Topics, ct)
			continue
		}
		if err := storage.ValidateTopicName(t.Topic); err != nil {
			ct.ErrorCode = protocol.ErrInvalidTopic
		} else if h.store.GetTopic(t.Topic) != nil {
			ct.ErrorCode = protocol.ErrTopicAlreadyExists
		} else if t.NumPartitions <= 0 {
			ct.ErrorCode = protocol.ErrInvalidPartitions
		} else if _, err := h.store.CreateTopic(t.Topic, int(t.NumPartitions)); err != nil {
			ct.ErrorCode = protocol.ErrUnknownServerError
		} else if isCompacted(t.Configs) {
			// Log compaction is requested; persist the topic flag.
			if err := h.store.MarkCompacted(t.Topic, true); err != nil {
				ct.ErrorCode = protocol.ErrUnknownServerError
			}
		}
		resp.Topics = append(resp.Topics, ct)
	}
	return protocol.EncodeCreateTopicsResponse(resp)
}

func (h *Handler) handleDeleteTopics(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeDeleteTopicsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.DeleteTopicsResponse{Version: version, ThrottleTimeMs: 0}
	for _, name := range req.TopicNames {
		dt := protocol.DeleteTopicsResponseTopic{Name: name, ErrorCode: protocol.ErrNone}
		if h.store.GetTopic(name) == nil {
			dt.ErrorCode = protocol.ErrUnknownTopicOrPartition
		} else if err := h.store.DeleteTopic(name); err != nil {
			dt.ErrorCode = protocol.ErrUnknownServerError
		}
		resp.Topics = append(resp.Topics, dt)
	}
	return protocol.EncodeDeleteTopicsResponse(resp)
}

func (h *Handler) handleFindCoordinator(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeFindCoordinatorRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.FindCoordinator(req)
	return protocol.EncodeFindCoordinatorResponse(resp)
}

func (h *Handler) handleJoinGroup(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeJoinGroupRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.JoinGroup(req)
	return protocol.EncodeJoinGroupResponse(resp)
}

func (h *Handler) handleSyncGroup(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeSyncGroupRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.SyncGroup(req)
	return protocol.EncodeSyncGroupResponse(resp)
}

func (h *Handler) handleHeartbeat(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeHeartbeatRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.Heartbeat(req)
	return protocol.EncodeHeartbeatResponse(resp)
}

func (h *Handler) handleLeaveGroup(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeLeaveGroupRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.LeaveGroup(req)
	return protocol.EncodeLeaveGroupResponse(resp)
}

func (h *Handler) handleOffsetCommit(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeOffsetCommitRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.OffsetCommit(req)
	return protocol.EncodeOffsetCommitResponse(resp)
}

func (h *Handler) handleOffsetFetch(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeOffsetFetchRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := h.coord.OffsetFetch(req)
	return protocol.EncodeOffsetFetchResponse(resp)
}

// longPoll blocks until data beyond fetchOffset is available or maxWait elapses.
func (h *Handler) longPoll(part *storage.Partition, fetchOffset int64, maxWait int32) {
	if maxWait <= 0 {
		return
	}
	if part.HighWatermark() > fetchOffset {
		return
	}
	deadline := time.Now().Add(time.Duration(maxWait) * time.Millisecond)
	for time.Now().Before(deadline) {
		if part.HighWatermark() > fetchOffset {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// isCompacted reports whether a topic creation request sets cleanup.policy=compact.
func isCompacted(configs []protocol.CreateTopicsConfig) bool {
	for _, c := range configs {
		if c.Name == "cleanup.policy" && c.Value != nil {
			for _, tok := range splitComma(*c.Value) {
				if tok == "compact" {
					return true
				}
			}
		}
	}
	return false
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			tok := strings.TrimSpace(s[start:i])
			if tok != "" {
				out = append(out, tok)
			}
			start = i + 1
		}
	}
	return out
}

// unsupportedVersion builds a response body reporting UNSUPPORTED_VERSION. For
// ApiVersions we include the supported keys so the client can downgrade.
func (h *Handler) unsupportedVersion(apiKey, version int16) ([]byte, error) {
	if apiKey == protocol.APKApiVersions {
		resp := &protocol.ApiVersionsResponse{
			Version:   protocol.MaxVersion(protocol.APKApiVersions),
			ErrorCode: protocol.ErrUnsupportedVersion,
			ApiKeys:   h.supportedKeys(),
		}
		return protocol.EncodeApiVersionsResponse(resp)
	}
	return nil, fmt.Errorf("unsupported version %d for api key %d", version, apiKey)
}
