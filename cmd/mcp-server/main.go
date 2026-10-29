// Command pocketkafka-mcp exposes the broker to AI agents over MCP (stdio).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/client"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

var (
	brokersFlag   = flag.String("b", "localhost:9092", "comma-separated broker addresses")
	registryFlag  = flag.String("registry", "http://localhost:8081", "schema registry base URL")
	timeoutFlag   = flag.Duration("timeout", 10*time.Second, "per-call broker timeout")
	allowWriteFlg = flag.Bool("allow-write", false, "enable write tools (produce, create/delete, reset)")
	authTokenFlg  = flag.String("auth-token", "", "require token in initialize params (env POCKETKAFKA_MCP_TOKEN)")
	webFlag       = flag.String("web", "http://localhost:8080", "web UI base URL for fleet/mqtt/admin (env POCKETKAFKA_WEB_URL)")
	webTokenFlag  = flag.String("web-token", "", "bearer token for web API (env POCKETKAFKA_WEB_TOKEN)")
)

const protoVersion = "2025-06-18"

var version = "dev"

type rpcReq struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcErr         `json:"error,omitempty"`
}

type rpcErr struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type server struct {
	kc         *client.KafkaClient
	registry   string
	web        string
	webToken   string
	http       *http.Client
	allowWrite bool
	authToken  string
	authed     bool
}

func (s *server) checkAuth(req rpcReq) bool {
	if s.authToken == "" {
		return true
	}
	var p struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return false
	}
	if p.Token == s.authToken {
		s.authed = true
		return true
	}
	return false
}

func main() {
	flag.Parse()
	if *authTokenFlg == "" {
		*authTokenFlg = os.Getenv("POCKETKAFKA_MCP_TOKEN")
	}
	if *webFlag == "" {
		*webFlag = os.Getenv("POCKETKAFKA_WEB_URL")
	}
	if *webTokenFlag == "" {
		*webTokenFlag = os.Getenv("POCKETKAFKA_WEB_TOKEN")
	}
	kc, err := client.NewClient(split(*brokersFlag), "pocketkafka-mcp")
	if err != nil {
		fatal(-32000, "connect broker: "+err.Error())
	}
	defer kc.Close()
	s := &server{kc: kc, registry: strings.TrimRight(*registryFlag, "/"),
		web:        strings.TrimRight(*webFlag, "/"),
		webToken:   *webTokenFlag,
		http:       &http.Client{Timeout: *timeoutFlag},
		allowWrite: *allowWriteFlg, authToken: *authTokenFlg}
	s.loop(os.Stdin, os.Stdout)
}

func (s *server) loop(r io.Reader, w io.Writer) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	out := bufio.NewWriter(w)
	defer out.Flush()
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		resp := s.dispatch(line)
		b, _ := json.Marshal(resp)
		out.Write(b)
		out.WriteByte('\n')
		out.Flush()
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "mcp: stdin: "+err.Error())
	}
}

func (s *server) dispatch(line []byte) rpcResp {
	var req rpcReq
	if err := json.Unmarshal(line, &req); err != nil {
		return rpcResp{JSONRPC: "2.0", Error: &rpcErr{Code: -32700, Message: "parse error"}}
	}
	if req.JSONRPC != "2.0" {
		return errResp(req, -32600, "invalid request: jsonrpc must be 2.0", nil)
	}
	switch req.Method {
	case "initialize":
		if !s.checkAuth(req) {
			return errResp(req, -32001, "unauthorized: bad or missing token", nil)
		}
		return okResp(req, map[string]any{
			"protocolVersion": protoVersion,
			"serverInfo":      map[string]any{"name": "pocketkafka-mcp", "version": version},
			"capabilities": map[string]any{
				"tools":     map[string]any{},
				"resources": map[string]any{},
				"prompts":   map[string]any{},
			},
		})
	case "ping":
		return okResp(req, map[string]any{})
	case "tools/list":
		return okResp(req, map[string]any{"tools": toolDefs()})
	case "tools/call":
		return s.call(req)
	case "resources/list":
		return okResp(req, map[string]any{"resources": s.listResources()})
	case "resources/read":
		return s.readResource(req)
	case "prompts/list":
		return okResp(req, map[string]any{"prompts": promptDefs()})
	case "prompts/get":
		return s.getPrompt(req)
	default:
		return errResp(req, -32601, "unknown method: "+req.Method, nil)
	}
}

type callParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func (s *server) call(req rpcReq) rpcResp {
	var p callParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return errResp(req, -32602, "invalid params: need {name, arguments}", nil)
	}
	args := p.Arguments
	if args == nil {
		args = map[string]any{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()
	_ = ctx

	out, callErr := s.runTool(p.Name, args)
	if callErr != nil {
		return errResp(req, callErr.code, callErr.msg, callErr.data)
	}
	return okResp(req, map[string]any{
		"content": []any{map[string]any{"type": "text", "text": toJSON(out)}},
	})
}

type callError struct {
	code int
	msg  string
	data interface{}
}

func (e *callError) Error() string { return e.msg }

func brokerErr(err error) *callError {
	return &callError{code: -32000, msg: err.Error()}
}

func paramErr(format string, a ...interface{}) *callError {
	return &callError{code: -32602, msg: fmt.Sprintf(format, a...)}
}

func (s *server) runTool(name string, args map[string]any) (interface{}, *callError) {
	switch name {
	case "list_topics":
		return s.listTopics()
	case "describe_topic":
		t, _ := args["topic"].(string)
		if t == "" {
			return nil, paramErr("missing required param: topic (string)")
		}
		return s.describeTopic(t)
	case "fetch_messages":
		return s.fetchMessages(args)
	case "list_consumer_groups":
		return s.listGroups()
	case "describe_consumer_group":
		g, _ := args["group_id"].(string)
		if g == "" {
			return nil, paramErr("missing required param: group_id (string)")
		}
		return s.describeGroup(g)
	case "list_schemas":
		return s.registryGet("/subjects", nil)
	case "get_schema":
		return s.getSchema(args)
	case "broker_health":
		return s.health()
	case "produce_message":
		return s.produce(args)
	case "create_topic":
		return s.createTopic(args)
	case "delete_topic":
		return s.deleteTopic(args)
	case "register_schema":
		return s.registerSchema(args)
	case "reset_consumer_offset":
		return s.resetOffset(args)
	case "delete_consumer_group":
		return s.deleteGroup(args)
	case "fleet_status":
		return s.webGet("/api/v1/fleet/status")
	case "fleet_list_servers":
		return s.webGet("/api/v1/fleet/servers")
	case "fleet_get_server":
		id, _ := args["server_id"].(string)
		if id == "" {
			return nil, paramErr("missing required param: server_id (string)")
		}
		return s.webGet("/api/v1/fleet/servers/" + id)
	case "fleet_list_containers":
		return s.webGet("/api/v1/fleet/containers")
	case "fleet_container_logs":
		id, _ := args["container_id"].(string)
		if id == "" {
			return nil, paramErr("missing required param: container_id (string)")
		}
		return s.webGet("/api/v1/fleet/containers/" + id + "/logs")
	case "fleet_list_jobs":
		return s.webGet("/api/v1/fleet/jobs")
	case "fleet_get_job":
		id, _ := args["job_id"].(string)
		if id == "" {
			return nil, paramErr("missing required param: job_id (string)")
		}
		return s.webGet("/api/v1/fleet/jobs/" + id)
	case "fleet_audit_events":
		return s.webGet("/api/v1/fleet/audit-events")
	case "fleet_container_start":
		return s.fleetContainerAction(args, "start")
	case "fleet_container_stop":
		return s.fleetContainerAction(args, "stop")
	case "fleet_container_restart":
		return s.fleetContainerAction(args, "restart")
	case "mqtt_status":
		return s.webGet("/api/v1/mqtt")
	case "mqtt_publish":
		return s.mqttPublish(args)
	default:
		return nil, &callError{code: -32601, msg: "unknown tool: " + name}
	}
}

func (s *server) needWrite() *callError {
	if !s.allowWrite {
		return &callError{code: -32002, msg: "write tools disabled: restart with --allow-write"}
	}
	return nil
}

func (s *server) audit(tool string, args map[string]any) {
	b, _ := json.Marshal(map[string]any{"time": time.Now().UTC().Format(time.RFC3339), "tool": tool, "args": args})
	fmt.Fprintln(os.Stderr, "mcp-audit "+string(b))
}

func okResp(req rpcReq, result interface{}) rpcResp {
	return rpcResp{JSONRPC: "2.0", ID: req.ID, Result: result}
}

func errResp(req rpcReq, code int, msg string, data interface{}) rpcResp {
	return rpcResp{JSONRPC: "2.0", ID: req.ID, Error: &rpcErr{Code: code, Message: msg, Data: data}}
}

func fatal(code int, msg string) {
	b, _ := json.Marshal(rpcResp{JSONRPC: "2.0", Error: &rpcErr{Code: code, Message: msg}})
	fmt.Println(string(b))
	os.Exit(1)
}

func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func numArg(args map[string]any, key string, def int64) int64 {
	v, ok := args[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return def
}

// Caps anti-ledakan-konteks.
const (
	maxLimit    = 100
	defaultCap  = 20
	maxValueLen = 4096
)

// --- tools ---

func (s *server) listTopics() (interface{}, *callError) {
	names := s.kc.ListTopics()
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]any{"name": n, "partitions": len(s.kc.Partitions(n))})
	}
	return out, nil
}

type partInfo struct {
	Partition int32 `json:"partition"`
	Leader    int32 `json:"leader"`
	Earliest  int64 `json:"earliest"`
	LEO       int64 `json:"log_end_offset"`
}

func (s *server) describeTopic(topic string) (interface{}, *callError) {
	parts := s.kc.Partitions(topic)
	if parts == nil {
		return nil, &callError{code: -32000, msg: "unknown topic: " + topic}
	}
	out := make([]partInfo, 0, len(parts))
	for _, p := range parts {
		earliest, err := s.kc.QueryOffset(topic, p.PartitionID, -2)
		if err != nil {
			return nil, brokerErr(err)
		}
		leo, err := s.kc.QueryOffset(topic, p.PartitionID, -1)
		if err != nil {
			return nil, brokerErr(err)
		}
		out = append(out, partInfo{Partition: p.PartitionID, Leader: p.LeaderID, Earliest: earliest, LEO: leo})
	}
	return map[string]any{"topic": topic, "partitions": out}, nil
}

type fetchedMsg struct {
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Truncated bool              `json:"truncated,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

func (s *server) fetchMessages(args map[string]any) (interface{}, *callError) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return nil, paramErr("missing required param: topic (string)")
	}
	partition := int32(numArg(args, "partition", 0))
	limit := numArg(args, "limit", defaultCap)
	if limit <= 0 {
		limit = defaultCap
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	var start int64
	var haveStart bool
	if v, ok := args["offset"]; ok {
		switch n := v.(type) {
		case float64:
			start, haveStart = int64(n), true
		case int64:
			start, haveStart = n, true
		}
	}
	if !haveStart {
		leo, err := s.kc.QueryOffset(topic, partition, -1)
		if err != nil {
			return nil, brokerErr(err)
		}
		earliest, err := s.kc.QueryOffset(topic, partition, -2)
		if err != nil {
			return nil, brokerErr(err)
		}
		start = leo - limit
		if start < earliest {
			start = earliest
		}
	}
	msgs, err := s.rawFetch(topic, partition, start, int(limit))
	if err != nil {
		return nil, brokerErr(err)
	}
	return msgs, nil
}

// rawFetch pakai FetchRequest mentah: tanpa join group, tanpa commit offset.
func (s *server) rawFetch(topic string, partition int32, start int64, limit int) ([]fetchedMsg, error) {
	ver := s.kc.Version(protocol.APKFetch)
	req := &protocol.FetchRequest{
		Version:       ver,
		ReplicaID:     -1,
		MaxWaitMillis: 100,
		MinBytes:      1,
		MaxBytes:      1 << 20,
		Topics: []protocol.FetchRequestTopic{{
			Topic: topic,
			Partitions: []protocol.FetchRequestPartition{{
				Partition: partition, FetchOffset: start, PartitionMaxBytes: 1 << 20,
			}},
		}},
	}
	body, err := protocol.EncodeFetchRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := s.kc.RoundTrip(protocol.APKFetch, ver, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeFetchResponse(ver, respBody)
	if err != nil {
		return nil, err
	}
	out := []fetchedMsg{}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != protocol.ErrNone {
				return nil, &client.KafkaError{Code: p.ErrorCode}
			}
			raw := p.Records
			for pos := 0; pos < len(raw) && len(out) < limit; {
				h, err := storage.ParseRecordBatchHeader(raw[pos:])
				if err != nil {
					break
				}
				total := int(storage.BatchTotalSize(h))
				if pos+total > len(raw) {
					break
				}
				b, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
				pos += total
				if err != nil {
					continue
				}
				for _, rec := range b.Records {
					val := string(rec.Value)
					trunc := false
					if len(val) > maxValueLen {
						val, trunc = val[:maxValueLen], true
					}
					hdrs := map[string]string{}
					for _, h := range rec.Headers {
						hdrs[h.Key] = string(h.Value)
					}
					out = append(out, fetchedMsg{
						Offset: b.BaseOffset + int64(rec.OffsetDelta),
						Key:    string(rec.Key), Value: val, Truncated: trunc, Headers: hdrs,
					})
					if len(out) >= limit {
						return out, nil
					}
				}
			}
		}
	}
	return out, nil
}

func (s *server) listGroups() (interface{}, *callError) {
	ids, err := s.kc.ListGroups()
	if err != nil {
		return nil, brokerErr(err)
	}
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		info, err := s.kc.DescribeGroup(id)
		if err != nil {
			continue
		}
		out = append(out, map[string]any{
			"group_id": info.GroupID, "state": info.State, "members": info.Members,
		})
	}
	return out, nil
}

// describeGroup tiru logika lag kctl/groups.go.
func (s *server) describeGroup(gid string) (interface{}, *callError) {
	info, err := s.kc.DescribeGroup(gid)
	if err != nil {
		return nil, brokerErr(err)
	}
	var rows []map[string]any
	for _, t := range s.kc.ListTopics() {
		for _, p := range s.kc.Partitions(t) {
			committed, err := s.kc.CommittedOffset(gid, t, p.PartitionID)
			if err != nil || committed < 0 {
				continue
			}
			leo, err := s.kc.QueryOffset(t, p.PartitionID, -1)
			if err != nil {
				continue
			}
			lag := leo - committed
			if lag < 0 {
				lag = 0
			}
			rows = append(rows, map[string]any{
				"topic": t, "partition": p.PartitionID,
				"current_offset": committed, "log_end_offset": leo, "lag": lag,
			})
		}
	}
	return map[string]any{
		"group_id": gid, "state": info.State, "members": info.Members, "offsets": rows,
	}, nil
}

func (s *server) getSchema(args map[string]any) (interface{}, *callError) {
	if id, ok := args["id"]; ok {
		var n int64
		switch v := id.(type) {
		case float64:
			n = int64(v)
		case int64:
			n = v
		default:
			return nil, paramErr("param id must be a number")
		}
		return s.registryGet(fmt.Sprintf("/schemas/ids/%d", n), nil)
	}
	subj, _ := args["subject"].(string)
	if subj == "" {
		return nil, paramErr("provide subject (+version) or id")
	}
	ver := "latest"
	if v, ok := args["version"]; ok {
		switch n := v.(type) {
		case string:
			ver = n
		case float64:
			ver = fmt.Sprintf("%d", int64(n))
		}
	}
	return s.registryGet("/subjects/"+subj+"/versions/"+ver, nil)
}

// --- write tools (gated --allow-write) ---

func (s *server) produce(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	topic, _ := args["topic"].(string)
	if topic == "" {
		return nil, paramErr("missing required param: topic (string)")
	}
	val, _ := args["value"].(string)
	if _, ok := args["value"]; !ok {
		return nil, paramErr("missing required param: value (string)")
	}
	key, _ := args["key"].(string)
	part := int32(numArg(args, "partition", 0))
	prod := client.NewProducer(s.kc, client.DefaultProducerConfig())
	defer prod.Close()
	// ponytail: partition pinning belakangan — SendSync default partisi 0; cekap untuk event tunggal.
	off, err := prod.SendSync(context.Background(), &client.Message{
		Topic: topic, Key: []byte(key), Value: []byte(val), Partition: part,
	})
	if err != nil {
		return nil, brokerErr(err)
	}
	s.audit("produce_message", map[string]any{"topic": topic, "offset": off})
	return map[string]any{"topic": topic, "partition": part, "offset": off}, nil
}

func (s *server) createTopic(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	topic, _ := args["topic"].(string)
	if topic == "" {
		return nil, paramErr("missing required param: topic (string)")
	}
	parts := int(numArg(args, "partitions", 1))
	if err := s.kc.CreateTopic(topic, parts); err != nil {
		return nil, brokerErr(err)
	}
	s.audit("create_topic", args)
	return map[string]any{"topic": topic, "partitions": parts}, nil
}

func (s *server) deleteTopic(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	topic, _ := args["topic"].(string)
	if topic == "" {
		return nil, paramErr("missing required param: topic (string)")
	}
	if err := s.kc.DeleteTopic(topic); err != nil {
		return nil, brokerErr(err)
	}
	s.audit("delete_topic", args)
	return map[string]any{"deleted": topic}, nil
}

func (s *server) registerSchema(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	subj, _ := args["subject"].(string)
	schema, _ := args["schema"].(string)
	if subj == "" || schema == "" {
		return nil, paramErr("missing required params: subject, schema (strings)")
	}
	stype, _ := args["schema_type"].(string)
	if stype == "" {
		stype = "AVRO"
	}
	body, _ := json.Marshal(map[string]any{"schema": schema, "schemaType": stype})
	resp, err := s.http.Post(s.registry+"/subjects/"+subj+"/versions",
		"application/vnd.schemaregistry.v1+json", strings.NewReader(string(body)))
	if err != nil {
		return nil, &callError{code: -32000, msg: "registry: " + err.Error()}
	}
	defer resp.Body.Close()
	var v interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, &callError{code: -32000, msg: "registry: bad response"}
	}
	if resp.StatusCode != 200 {
		return nil, &callError{code: -32000, msg: fmt.Sprintf("registry %d: %s", resp.StatusCode, toJSON(v))}
	}
	s.audit("register_schema", map[string]any{"subject": subj, "type": stype})
	return v, nil
}

func (s *server) resetOffset(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	gid, _ := args["group_id"].(string)
	topic, _ := args["topic"].(string)
	if gid == "" || topic == "" {
		return nil, paramErr("missing required params: group_id, topic (strings)")
	}
	// Tanpa offset eksplisit per partisi = tolak. Reset buta = data loss senyap.
	offs, ok := args["offsets"].(map[string]any)
	if !ok || len(offs) == 0 {
		return nil, paramErr("missing required param: offsets (map partition->offset); no blind reset")
	}
	for ps, ov := range offs {
		var part int64
		if _, err := fmt.Sscanf(ps, "%d", &part); err != nil {
			return nil, paramErr("bad partition key %q", ps)
		}
		var off int64
		switch n := ov.(type) {
		case float64:
			off = int64(n)
		default:
			return nil, paramErr("bad offset for partition %q", ps)
		}
		if err := s.kc.ResetOffset(gid, topic, int32(part), off); err != nil {
			return nil, brokerErr(err)
		}
	}
	s.audit("reset_consumer_offset", args)
	return map[string]any{"group_id": gid, "topic": topic, "offsets": offs}, nil
}

func (s *server) deleteGroup(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	gid, _ := args["group_id"].(string)
	if gid == "" {
		return nil, paramErr("missing required param: group_id (string)")
	}
	if err := s.kc.DeleteGroup(gid); err != nil {
		return nil, brokerErr(err)
	}
	s.audit("delete_consumer_group", args)
	return map[string]any{"deleted": gid}, nil
}

func (s *server) registryGet(path string, _ map[string]any) (interface{}, *callError) {
	resp, err := s.http.Get(s.registry + path)
	if err != nil {
		return nil, &callError{code: -32000, msg: "registry: " + err.Error()}
	}
	defer resp.Body.Close()
	var v interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, &callError{code: -32000, msg: "registry: bad response"}
	}
	if resp.StatusCode != 200 {
		return nil, &callError{code: -32000, msg: fmt.Sprintf("registry %d: %s", resp.StatusCode, toJSON(v))}
	}
	return v, nil
}

// webGet panggil web API :8080 (fleet, mqtt, admin). Hormati auth web.
func (s *server) webGet(path string) (interface{}, *callError) {
	return s.webCall("GET", path, nil)
}

func (s *server) webCall(method, path string, body interface{}) (interface{}, *callError) {
	if s.web == "" {
		return nil, &callError{code: -32000, msg: "web API not configured (use --web)"}
	}
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = strings.NewReader(string(b))
	}
	req, err := http.NewRequest(method, s.web+path, rdr)
	if err != nil {
		return nil, &callError{code: -32000, msg: "web: " + err.Error()}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.webToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.webToken)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, &callError{code: -32000, msg: "web: " + err.Error()}
	}
	defer resp.Body.Close()
	var v interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, &callError{code: -32000, msg: "web: bad response"}
	}
	if resp.StatusCode >= 400 {
		return nil, &callError{code: -32000, msg: fmt.Sprintf("web %d: %s", resp.StatusCode, toJSON(v))}
	}
	return v, nil
}

func (s *server) fleetContainerAction(args map[string]any, action string) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	id, _ := args["container_id"].(string)
	if id == "" {
		return nil, paramErr("missing required param: container_id (string)")
	}
	out, cerr := s.webCall("POST", "/api/v1/fleet/containers/"+id+"/"+action, map[string]any{})
	if cerr != nil {
		return nil, cerr
	}
	s.audit("fleet_container_"+action, args)
	return out, nil
}

func (s *server) mqttPublish(args map[string]any) (interface{}, *callError) {
	if cerr := s.needWrite(); cerr != nil {
		return nil, cerr
	}
	topic, _ := args["topic"].(string)
	if topic == "" {
		return nil, paramErr("missing required param: topic (string)")
	}
	payload, _ := args["payload"].(string)
	out, cerr := s.webCall("POST", "/api/v1/mqtt/publish", map[string]any{
		"topic": topic, "payload": payload, "qos": int(numArg(args, "qos", 0)),
	})
	if cerr != nil {
		return nil, cerr
	}
	s.audit("mqtt_publish", map[string]any{"topic": topic})
	return out, nil
}

func (s *server) health() (interface{}, *callError) {
	names := s.kc.ListTopics()
	parts, msgs := 0, int64(0)
	for _, n := range names {
		for _, p := range s.kc.Partitions(n) {
			parts++
			leo, err := s.kc.QueryOffset(n, p.PartitionID, -1)
			if err != nil {
				continue
			}
			earliest, err := s.kc.QueryOffset(n, p.PartitionID, -2)
			if err != nil {
				continue
			}
			msgs += leo - earliest
		}
	}
	return map[string]any{
		"broker": "pocketkafka", "topics": len(names),
		"partitions": parts, "messages": msgs,
	}, nil
}

func toolDefs() []tool {
	obj := func(props map[string]any, req ...string) map[string]any {
		return map[string]any{"type": "object", "properties": props, "required": req}
	}
	str := func(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
	num := func(desc string) map[string]any { return map[string]any{"type": "number", "description": desc} }
	return []tool{
		{"list_topics", "List all topics with partition counts", obj(map[string]any{})},
		{"describe_topic", "Partition layout + earliest/LEO offsets per partition", obj(map[string]any{"topic": str("topic name")}, "topic")},
		{"fetch_messages", "Read messages without joining a group (no commit)", obj(map[string]any{
			"topic":     str("topic name"),
			"partition": num("partition, default 0"),
			"offset":    num("start offset; omit = tail"),
			"limit":     num("max messages, default 20, max 100"),
		}, "topic")},
		{"list_consumer_groups", "List consumer groups with state and members", obj(map[string]any{})},
		{"describe_consumer_group", "Group members + committed offsets + lag", obj(map[string]any{"group_id": str("group id")}, "group_id")},
		{"list_schemas", "List schema registry subjects", obj(map[string]any{})},
		{"get_schema", "Fetch a schema by subject+version or by id", obj(map[string]any{
			"subject": str("subject name"), "version": str("version or 'latest'"), "id": num("schema id"),
		})},
		{"broker_health", "Cluster summary: topics, partitions, message counts", obj(map[string]any{})},
		{"produce_message", "Publish one event (needs --allow-write)", obj(map[string]any{
			"topic": str("topic name"), "value": str("event payload"),
			"key": str("optional key"), "partition": num("partition, default 0"),
		}, "topic", "value")},
		{"create_topic", "Create topic (needs --allow-write)", obj(map[string]any{
			"topic": str("topic name"), "partitions": num("partition count, default 1"),
		}, "topic")},
		{"delete_topic", "Delete topic, destructive (needs --allow-write + host confirm)", obj(map[string]any{
			"topic": str("topic name"),
		}, "topic")},
		{"register_schema", "Register AVRO/JSON schema (needs --allow-write)", obj(map[string]any{
			"subject": str("subject name"), "schema": str("schema JSON"),
			"schema_type": str("AVRO or JSON, default AVRO"),
		}, "subject", "schema")},
		{"reset_consumer_offset", "Reset explicit partition offsets, destructive (needs --allow-write + host confirm)", obj(map[string]any{
			"group_id": str("group id"), "topic": str("topic name"),
			"offsets": map[string]any{"type": "object", "description": "partition -> offset, e.g. {\"0\": 42}"},
		}, "group_id", "topic", "offsets")},
		{"delete_consumer_group", "Delete group (needs --allow-write)", obj(map[string]any{
			"group_id": str("group id"),
		}, "group_id")},
		{"fleet_status", "Fleet controller status (servers, agents, jobs)", obj(map[string]any{})},
		{"fleet_list_servers", "List fleet-managed servers", obj(map[string]any{})},
		{"fleet_get_server", "Server detail + agent", obj(map[string]any{
			"server_id": str("server id"),
		}, "server_id")},
		{"fleet_list_containers", "List containers on fleet hosts", obj(map[string]any{})},
		{"fleet_container_logs", "Container logs", obj(map[string]any{
			"container_id": str("container id"),
		}, "container_id")},
		{"fleet_list_jobs", "List fleet jobs", obj(map[string]any{})},
		{"fleet_get_job", "Job detail + result", obj(map[string]any{
			"job_id": str("job id"),
		}, "job_id")},
		{"fleet_audit_events", "Fleet audit trail", obj(map[string]any{})},
		{"fleet_container_start", "Start container (needs --allow-write)", obj(map[string]any{
			"container_id": str("container id"),
		}, "container_id")},
		{"fleet_container_stop", "Stop container (needs --allow-write)", obj(map[string]any{
			"container_id": str("container id"),
		}, "container_id")},
		{"fleet_container_restart", "Restart container (needs --allow-write)", obj(map[string]any{
			"container_id": str("container id"),
		}, "container_id")},
		{"mqtt_status", "MQTT bridge status + clients", obj(map[string]any{})},
		{"mqtt_publish", "Publish MQTT message, bridged to Kafka (needs --allow-write)", obj(map[string]any{
			"topic": str("mqtt topic"), "payload": str("payload"),
			"qos": num("qos 0-2, default 0"),
		}, "topic", "payload")},
	}
}

// --- resources ---

func (s *server) listResources() []map[string]any {
	res := []map[string]any{
		{"uri": "kafka://topics", "name": "topics", "description": "All topics"},
		{"uri": "kafka://schemas", "name": "schemas", "description": "Schema subjects"},
		{"uri": "kafka://health", "name": "health", "description": "Cluster summary"},
		{"uri": "fleet://status", "name": "fleet status", "description": "Fleet controller status"},
		{"uri": "fleet://servers", "name": "fleet servers", "description": "Fleet-managed servers"},
		{"uri": "mqtt://status", "name": "mqtt status", "description": "MQTT bridge status"},
	}
	for _, n := range s.kc.ListTopics() {
		res = append(res, map[string]any{
			"uri": "kafka://topics/" + n, "name": "topic " + n,
			"description": "Metadata + offsets for " + n,
		})
	}
	if v, cerr := s.registryGet("/subjects", nil); cerr == nil {
		if list, ok := v.([]interface{}); ok {
			for _, item := range list {
				if subj, ok := item.(string); ok {
					res = append(res, map[string]any{
						"uri": "kafka://schemas/" + subj, "name": "schema " + subj,
						"description": "Latest schema for " + subj,
					})
				}
			}
		}
	}
	return res
}

func textResource(uri, text string) map[string]any {
	return map[string]any{
		"contents": []any{map[string]any{"uri": uri, "mimeType": "application/json", "text": text}},
	}
}

func (s *server) readResource(req rpcReq) rpcResp {
	var p struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil || p.URI == "" {
		return errResp(req, -32602, "invalid params: need {uri}", nil)
	}
	uri := p.URI
	var (
		out  interface{}
		cerr *callError
	)
	switch {
	case uri == "kafka://topics":
		out, cerr = s.listTopics()
	case uri == "kafka://health":
		out, cerr = s.health()
	case uri == "kafka://schemas":
		out, cerr = s.registryGet("/subjects", nil)
	case strings.HasPrefix(uri, "kafka://topics/"):
		out, cerr = s.describeTopic(strings.TrimPrefix(uri, "kafka://topics/"))
	case strings.HasPrefix(uri, "kafka://schemas/"):
		subj := strings.TrimPrefix(uri, "kafka://schemas/")
		out, cerr = s.registryGet("/subjects/"+subj+"/versions/latest", nil)
	case uri == "fleet://status":
		out, cerr = s.webGet("/api/v1/fleet/status")
	case uri == "fleet://servers":
		out, cerr = s.webGet("/api/v1/fleet/servers")
	case uri == "mqtt://status":
		out, cerr = s.webGet("/api/v1/mqtt")
	default:
		return errResp(req, -32602, "unknown resource: "+uri, nil)
	}
	if cerr != nil {
		return errResp(req, cerr.code, cerr.msg, cerr.data)
	}
	return okResp(req, textResource(uri, toJSON(out)))
}

// --- prompts ---

func promptDefs() []map[string]any {
	return []map[string]any{
		{"name": "investigate-lag", "description": "Diagnose consumer lag step by step",
			"arguments": []any{map[string]any{"name": "group_id", "required": true}}},
		{"name": "inspect-topic", "description": "Inspect a topic: layout, schema, sample messages",
			"arguments": []any{map[string]any{"name": "topic", "required": true}}},
		{"name": "publish-event", "description": "Guide for publishing + verifying an event (needs --allow-write)",
			"arguments": []any{map[string]any{"name": "topic", "required": true}}},
		{"name": "fleet-health", "description": "Check fleet: status, failing jobs, unhealthy servers",
			"arguments": []any{}},
	}
}

func (s *server) getPrompt(req rpcReq) rpcResp {
	var p struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil || p.Name == "" {
		return errResp(req, -32602, "invalid params: need {name, arguments?}", nil)
	}
	arg := ""
	if p.Arguments != nil {
		if v, ok := p.Arguments["group_id"].(string); ok {
			arg = v
		}
		if v, ok := p.Arguments["topic"].(string); ok {
			arg = v
		}
	}
	var text string
	switch p.Name {
	case "investigate-lag":
		text = "Diagnose lag for group " + arg + ": 1) call describe_consumer_group, " +
			"2) for lagging partitions call describe_topic, " +
			"3) call fetch_messages on the tail to inspect recent records."
	case "inspect-topic":
		text = "Inspect topic " + arg + ": 1) call describe_topic, " +
			"2) call list_schemas and get_schema if a matching subject exists, " +
			"3) call fetch_messages with limit 5 for a sample."
	case "publish-event":
		text = "Publish to " + arg + ": write tools are not enabled in read-only mode. " +
			"Restart pocketkafka-mcp with --allow-write, then use produce flow and verify with fetch_messages."
	case "fleet-health":
		text = "Check fleet health: 1) call fleet_status, " +
			"2) call fleet_list_jobs and flag failed ones, " +
			"3) call fleet_list_servers and check for offline agents."
	default:
		return errResp(req, -32602, "unknown prompt: "+p.Name, nil)
	}
	return okResp(req, map[string]any{
		"messages": []any{map[string]any{
			"role":    "user",
			"content": map[string]any{"type": "text", "text": text},
		}},
	})
}
