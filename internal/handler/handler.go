// Package handler implements the per-API-request handlers for the broker,
// decoding requests with pkg/protocol and producing responses backed by the
// storage engine and the group coordinator.
package handler

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
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
		{ApiKey: protocol.APKDescribeGroups, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDescribeGroups)},
		{ApiKey: protocol.APKListGroups, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKListGroups)},
		{ApiKey: protocol.APKSaslHandshake, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSaslHandshake)},
		{ApiKey: protocol.APKApiVersions, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKApiVersions)},
		{ApiKey: protocol.APKCreateTopics, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKCreateTopics)},
		{ApiKey: protocol.APKDeleteTopics, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDeleteTopics)},
		{ApiKey: protocol.APKInitProducerID, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKInitProducerID)},
		{ApiKey: protocol.APKAddPartitionsToTxn, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKAddPartitionsToTxn)},
		{ApiKey: protocol.APKAddOffsetsToTxn, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKAddOffsetsToTxn)},
		{ApiKey: protocol.APKEndTxn, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKEndTxn)},
		{ApiKey: protocol.APKSaslAuthenticate, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKSaslAuthenticate)},
		{ApiKey: protocol.APKDeleteGroups, MinVersion: 0, MaxVersion: protocol.MaxVersion(protocol.APKDeleteGroups)},
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
	case protocol.APKListGroups:
		return h.handleListGroups(version, body)
	case protocol.APKDescribeGroups:
		return h.handleDescribeGroups(version, body)
	case protocol.APKDeleteGroups:
		return h.handleDeleteGroups(version, body)
	case protocol.APKInitProducerID:
		return h.handleInitProducerID(version, body)
	case protocol.APKAddPartitionsToTxn:
		return h.handleAddPartitionsToTxn(version, body)
	case protocol.APKAddOffsetsToTxn:
		return h.handleAddOffsetsToTxn(version, body)
	case protocol.APKEndTxn:
		return h.handleEndTxn(version, body)
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
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}

			base, aerr := h.appendToPartition(part, p.Records)
			if aerr != nil {
				rp.ErrorCode = protocol.ErrCorruptMessage
				rp.BaseOffset = -1
			} else {
				rp.BaseOffset = base
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeProduceResponse(resp)
}

// appendToPartition validates and persists one RecordBatch for a produce
// request. It handles compression (decompressing the batch before storing it so
// the log always stores uncompressed payloads) and idempotent producer
// sequence validation.
func (h *Handler) appendToPartition(part *storage.Partition, raw []byte) (int64, error) {
	if len(raw) < 61 {
		return -1, fmt.Errorf("record batch too short")
	}
	// Read the compression codec from the attributes field (bytes 21-23)
	// without decoding the (possibly compressed) records first.
	attr := int16(binary.BigEndian.Uint16(raw[21:23]))
	if codec := protocol.CompressionCodec(attr); codec != protocol.CompressionNone {
		records, derr := protocol.DecompressRecordBatch(raw[61:], attr)
		if derr != nil {
			return -1, fmt.Errorf("decompress: %w", derr)
		}
		raw = rebuildBatch(raw, records)
	}

	batch, err := protocol.DecodeRecordBatch(raw)
	if err != nil {
		return -1, err
	}
	if batch.ProducerID >= 0 {
		// Idempotent producer path: sequence validation + dedup.
		return part.AppendIdempotent(batch.ProducerID, batch.ProducerEpoch, batch.BaseSequence, raw)
	}
	return part.Append(raw)
}

// rebuildBatch re-encodes a RecordBatch with decompressed records and a zeroed
// compression codec, preserving timestamps and producer metadata.
func rebuildBatch(raw []byte, records []byte) []byte {
	// Decode the decompressed records so we can re-encode them with codec 0.
	if len(records) < 4 {
		return raw
	}
	// The records payload is a sequence of varint-length-prefixed records; the
	// count lives in the header (bytes 57-61). Rebuild the header by hand.
	count := int32(binary.BigEndian.Uint32(raw[57:61]))
	rest := records
	decoded := make([]protocol.Record, 0, count)
	for i := int32(0); i < count && len(rest) > 0; i++ {
		length, n := binary.Varint(rest)
		if n <= 0 || int(length) < 0 || n+int(length) > len(rest) {
			break
		}
		rec, _, err := protocol.DecodeRecordBytes(rest[:n+int(length)])
		if err != nil {
			break
		}
		decoded = append(decoded, rec)
		rest = rest[n+int(length):]
	}
	if len(decoded) == 0 {
		return raw
	}
	b := &protocol.RecordBatch{
		PartitionLeaderEpoch: int32(binary.BigEndian.Uint32(raw[12:16])),
		Attributes:           int16(binary.BigEndian.Uint16(raw[21:23])) &^ 0x07,
		LastOffsetDelta:      int32(binary.BigEndian.Uint32(raw[23:27])),
		BaseTimestamp:        int64(binary.BigEndian.Uint64(raw[27:35])),
		MaxTimestamp:         int64(binary.BigEndian.Uint64(raw[35:43])),
		ProducerID:           int64(binary.BigEndian.Uint64(raw[43:51])),
		ProducerEpoch:        int16(binary.BigEndian.Uint16(raw[51:53])),
		BaseSequence:         int32(binary.BigEndian.Uint32(raw[53:57])),
		Records:              decoded,
	}
	out, err := protocol.EncodeRecordBatch(b)
	if err != nil {
		return raw
	}
	return out
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

// handleListGroups (Key 16) lists all registered consumer groups.
func (h *Handler) handleListGroups(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeListGroupsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.ListGroupsResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	for _, id := range h.coord.ListGroupIDs() {
		resp.Groups = append(resp.Groups, protocol.ListGroupsResponseGroup{
			GroupID:      id,
			ProtocolType: "consumer",
		})
	}
	return protocol.EncodeListGroupsResponse(resp)
}

// handleDescribeGroups (Key 15) returns per-group state, members and protocol.
func (h *Handler) handleDescribeGroups(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeDescribeGroupsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.DescribeGroupsResponse{Version: req.Version}
	for _, id := range req.GroupIDs {
		g := protocol.DescribeGroupsResponseGroup{GroupID: id}
		info := h.coord.DescribeGroup(id)
		if info == nil {
			g.ErrorCode = protocol.ErrGroupIDNotFound
			g.State = "Dead"
			resp.Groups = append(resp.Groups, g)
			continue
		}
		g.ErrorCode = protocol.ErrNone
		g.State = info.State
		g.ProtocolType = "consumer"
		g.Protocol = "range"
		for _, m := range h.coord.MembersDetail(id) {
			g.Members = append(g.Members, protocol.DescribeGroupsResponseMember{
				MemberID:         m.MemberID,
				ClientID:         m.ClientID,
				ClientHost:       m.ClientHost,
				MemberMetadata:   m.Metadata,
				MemberAssignment: m.Assignment,
			})
		}
		resp.Groups = append(resp.Groups, g)
	}
	return protocol.EncodeDescribeGroupsResponse(resp)
}

// handleDeleteGroups (Key 42) deletes empty groups.
func (h *Handler) handleDeleteGroups(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeDeleteGroupsRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.DeleteGroupsResponse{Version: req.Version}
	for _, id := range req.GroupIDs {
		g := protocol.DeleteGroupsResponseGroup{GroupID: id, ErrorCode: protocol.ErrNone}
		if err := h.coord.DeleteGroup(id); err != nil {
			g.ErrorCode = protocol.ErrNonEmptyGroup
			msg := "group is not empty"
			g.ErrorMessage = &msg
		}
		resp.Groups = append(resp.Groups, g)
	}
	return protocol.EncodeDeleteGroupsResponse(resp)
}

// handleInitProducerID (Key 22) allocates a producer ID and epoch for
// idempotent producers and transactions.
func (h *Handler) handleInitProducerID(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeInitProducerIdRequest(version, body)
	if err != nil {
		return nil, err
	}
	pid, epoch := h.coord.NextProducerID(req.TransactionalID)
	resp := &protocol.InitProducerIdResponse{
		Version:       req.Version,
		ErrorCode:     protocol.ErrNone,
		ProducerID:    pid,
		ProducerEpoch: epoch,
	}
	return protocol.EncodeInitProducerIdResponse(resp)
}

// handleAddPartitionsToTxn (Key 24) registers partitions with a transaction.
func (h *Handler) handleAddPartitionsToTxn(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeAddPartitionsToTxnRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.AddPartitionsToTxnResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	if !h.coord.ValidateProducer(req.TransactionalID, req.ProducerID, req.ProducerEpoch) {
		resp.ErrorCode = protocol.ErrTransactionCoordinatorFenced
		return protocol.EncodeAddPartitionsToTxnResponse(resp)
	}
	for _, t := range req.Topics {
		rt := protocol.AddPartitionsToTxnResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rt.Partitions = append(rt.Partitions, protocol.AddPartitionsToTxnResponsePartition{
				Partition: p,
				ErrorCode: protocol.ErrNone,
			})
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeAddPartitionsToTxnResponse(resp)
}

// handleAddOffsetsToTxn (Key 25) registers a consumer group with a transaction.
func (h *Handler) handleAddOffsetsToTxn(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeAddOffsetsToTxnRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.AddOffsetsToTxnResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	if !h.coord.ValidateProducer(req.TransactionalID, req.ProducerID, req.ProducerEpoch) {
		resp.ErrorCode = protocol.ErrTransactionCoordinatorFenced
	}
	return protocol.EncodeAddOffsetsToTxnResponse(resp)
}

// handleEndTxn (Key 26) commits or aborts a transaction.
func (h *Handler) handleEndTxn(version int16, body []byte) ([]byte, error) {
	req, err := protocol.DecodeEndTxnRequest(version, body)
	if err != nil {
		return nil, err
	}
	resp := &protocol.EndTxnResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	if !h.coord.ValidateProducer(req.TransactionalID, req.ProducerID, req.ProducerEpoch) {
		resp.ErrorCode = protocol.ErrTransactionCoordinatorFenced
	}
	return protocol.EncodeEndTxnResponse(resp)
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
