# Global Cluster Switcher Design

## Goal

Let an operator select the broker/cluster used by dashboard features, using a persistent global `Broker` dropdown like the supplied reference. Make the Clusters table actions visibly separate instead of rendering Open/Edit/Remove as a single touching button strip.

## Repository context

The embedded SPA is `web/dist/index.html`; `api()` currently calls same-origin endpoints. `internal/web/monitor.go` already resolves `GET /api/v1/health/overview?cluster=<name>` for registered peers and external Kafka clusters. Peer clusters are fetched over HTTP for Health using the stored cluster token; Kafka clusters are sampled by a read-only Kafka client. Other existing APIs address the local broker. Peer cluster tokens authorize reads only; mutations additionally require an authenticated user with ACL permission. Cluster endpoints and credentials are held in the server-side registry and sealed secret store.

## Approved scope and design decisions

- **Global selection:** Add an accessible `Broker` select to the shared top bar. Include the local broker and registered targets, labeled by cluster name. Persist the selected target in browser `localStorage`; default to local.
- **Target behavior:** Every supported view/API operation uses the selected cluster. Switching targets stops the old target's polling/live-tail, clears target-specific rendered and cached data, then loads the new target. An unreachable or unauthorized target stays selected and shows an error; never silently display or mutate the local broker instead.
- **Server-side routing:** The browser sends a registered cluster identity, never a target URL or stored secret. The Go server resolves that identity in its registry and performs bounded calls only to explicit, allowlisted API paths and methods. It must reject arbitrary paths/headers and must not follow redirects outside the configured peer origin. Requests to peers use only their configured URL. Existing cluster health tokens remain read-only and must not be elevated for mutations.
- **Peer authorization:** Add a distinct high-entropy API credential provisioned on the destination PocketKafka broker and bound there to a named existing principal. The target authenticates this bearer as that principal so its ACL and audit rules apply. Provision/revoke it through a target-admin-only path; store its verifier securely on the target and the matching secret in the source dashboard's existing sealed-secret store. The Clusters form must distinguish dashboard access credentials from the existing read-only health token. Do not forward the current dashboard's cookies or treat the health token as an administrator credential. The UI must clearly distinguish missing/denied peer access from network failure.
- **Kafka capabilities:** External Kafka remains read-only. Expose only features backed by an implemented and verified Kafka operation; disable unavailable controls/views with an explicit explanation. Never send write or PocketKafka-only requests to a Kafka cluster. The initial capability map must be based on actual implemented API support, not on the fact that Health contains topic/group summaries.
- **Local registry management:** The Clusters view edits this dashboard's own target registry and remains local-scoped, regardless of the global selection. The selected name must remain visible if that target is deleted or no longer registered; show an unavailable-target state until the operator selects a valid target.
- **Button spacing:** Render row actions in a dedicated flex wrapper with a consistent gap and wrapping at narrow widths. Preserve each existing Open/Edit/Remove action and authorization behavior.
- **Compatibility:** Preserve the single-file vanilla-JavaScript SPA and current routes. Keep cluster names as identifiers; do not introduce browser-side arbitrary URL access or expose stored credentials.

## Approaches considered

1. **Server-side capability-aware routing (selected):** Route supported operations through the same-origin Go server, enforce target registry and target ACLs, and show explicit unavailable states for Kafka capabilities that do not exist. This preserves secret boundaries and provides real target behavior rather than a visual-only selector.
2. **Browser-side direct calls to remote clusters:** Rejected. It would expose credentials, depend on peer CORS behavior, and allow browser-supplied targets to bypass the server's allowlisted registry.
3. **Health-only selector:** Rejected. It would not satisfy the requested global cluster selection.

## Interaction and failure behavior

- The selector is keyboard-operable, labeled `Broker`, and preserves the current selection across reloads.
- On target change, disconnect target-bound WebSockets before loading new data; stale responses from the previous target must not overwrite the new view.
- A request failure reports the selected target and error. No local fallback, stale data reuse, or fabricated empty-success state.
- Unsupported Kafka features are visibly unavailable and explain the limitation; controls must not remain actionable.
- If a saved target is removed, preserve its label as unavailable rather than selecting local implicitly.

## Architecture and data flow

The SPA's central API helper attaches the selected registered cluster identity to target-capable requests. The server validates that identity against the registry and dispatches by cluster kind: local requests keep the current handler path; PocketKafka peer requests use a constrained server-side API proxy with the new delegated peer principal; Kafka requests use explicit read-only adapters. The proxy forwards only approved API paths/methods and required headers, never browser cookies or arbitrary URLs. API routes for registry management remain local. WebSocket/live-tail selection follows the same target and capability rules. Each cluster kind exposes a capability map so unsupported pages/actions can be disabled before issuing requests.

Mutating peer requests are authorized at the receiving broker as the configured delegated principal, not by trusting the source broker's UI selection. Audit records use that principal. The target administrator provisions/revokes the credential and assigns its ACL; the source dashboard stores the secret sealed. Secrets never enter SPA markup, URLs, localStorage, or error text. The health aggregation token retains its existing read-only contract.

## Verification criteria

- The `Broker` dropdown lists local and registered clusters, persists selection, and switches data without retaining a previous target's values.
- Requests are routed only through registry-resolved identities; an unknown or deleted selection never reaches an arbitrary address and never falls back silently to local.
- Supported PocketKafka peer reads and writes reach that peer; denied ACLs remain denied, and the health-only token cannot perform mutations.
- Kafka external features are read-only and every unsupported view/action is disabled with a clear explanation; no Kafka mutation is sent.
- Switching while polling or tailing stops the previous target's work and ignores late responses.
- The Clusters registry screen continues to manage the local registry independently of the selected data target.
- Open/Edit/Remove remain functional and have visible, consistent spacing at desktop and narrow widths.
- The embedded SPA remains usable in dark/light themes and by keyboard; no new frontend runtime is introduced.

## Out of scope

- Making PocketKafka-only administration features exist on standard Kafka brokers.
- Browser-to-broker direct API access, CORS changes, or exposing cluster secrets to JavaScript.
- Replacing the local cluster registry with a remote/shared registry.
