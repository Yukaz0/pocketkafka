# PocketKafka UI Refactor Design

## Goal

Refactor the complete embedded PocketKafka SPA into a coherent control center for both broker operators and application developers. Preserve existing product capabilities and backend contracts. Deliver the redesign incrementally, beginning with the application shell and Overview, then migrate monitoring and developer views to the shared visual system.

## Repository context

The UI is a vanilla JavaScript SPA in `web/dist/index.html`, embedded by `web/web.go` from `web/dist` and served by the broker. Existing views include Connections, Data Browser, Topics, Consumer Groups, Schemas, Producer Studio, Dev Studio, Topology, Storage, Clusters, Health, Metrics, Broker Logs, and Security & ACL. Health history, cluster monitoring, and other view behavior already have API/UI contracts in `.hermes/health-view-contract.md`, `.hermes/multicluster-ui-contract.md`, and `.hermes/ui-refactor-contract.md`; redesign must preserve those contracts and current working behavior.

## Design decisions

- **Scope:** Redesign the full SPA, delivered in stages rather than a single high-risk cutover.
- **Navigation:** A collapsible, labeled sidebar. Group operational surfaces separately from developer tools. Collapsed mode becomes an icon rail; retain accessible names and visible keyboard focus.
- **Default destination:** Overview, combining the existing health and operational data. Do not invent backend data or imply unimplemented capabilities.
- **Overview composition:** Put broker/cluster health and actionable attention items near the top. Show broker status, active issue count, throughput, and consumer lag as balanced summary facts; follow with messages-per-second and total-lag trends, then relevant detail/entry points. Keep health and issues prominent without making all monitoring data visually subordinate.
- **Visual language:** Preserve PocketKafka's dark industrial foundation and blue accent. Dark and light themes are equally supported and persistent. Use blue for interaction/selection, not decoration; reserve green/amber/red for semantic status. Flat surfaces, 4px radius, no gradients, glass effects, glow, or decorative status pills.
- **Density and typography:** Compact technical layout for monitoring and data tables; maintain clear hierarchy, readable contrast, tabular alignment for broker data, and robust local font fallbacks. Use roomier layouts only where an editing workflow needs it.
- **Compatibility:** Vanilla frontend, embedded single-page distribution, no backend/API contract changes, and no new feature claims. Preserve existing refresh, sorting, access control, and error/empty/loading behavior unless a specific view's contract calls for a change.

## Navigation structure

Use clear groups, with all current destinations retained:

- **Overview:** Overview.
- **Operations:** Health, Clusters, Metrics, Broker Logs, Security & ACL.
- **Developer tools:** Data Browser, Topics, Consumer Groups, Schemas, Producer Studio, Dev Studio, Topology, Storage.
- **Connections:** Keep connection management discoverable as an application-level entry; its exact placement should not imply a backend behavior change.

The sidebar must remain usable in expanded/collapsed states, keyboard navigation, narrow viewports, and both themes. On small screens it may become a drawer, but must not create page-level horizontal overflow.

## Staged delivery

1. **Foundation and Overview:** Establish shared shell, responsive collapsible navigation, theme tokens/toggle/persistence, common page-header and surface styles, and the Overview composition. Keep the old routes and functionality available during migration.
2. **Operations:** Migrate Health and Clusters to shared shell components while retaining polling, in-place updates, cluster selection, rate-reliability semantics, and error/loading/empty states. Align Metrics and Broker Logs; keep Security & ACL permissions and behavior intact.
3. **Developer tools:** Migrate Connections, Data Browser, Topics, Consumer Groups, Schemas, Producer Studio, Dev Studio, Topology, and Storage. Preserve each existing action, data contract, and relevant keyboard behavior. Remove obsolete UI code only after its replacement and callers are migrated.

Each stage should leave the embedded app usable; no dead controls, placeholder screens, or backend-dependent feature stubs.

## Interaction and state requirements

- Theme selection persists and applies coherently to all migrated views; both themes must maintain legible contrast and status differentiation.
- Sidebar collapse state should not obscure labels or keyboard accessibility. Navigation reflects the active view and remains available on mobile.
- Existing view controls and API-driven state retain their current semantics. Loading, error, and empty states explain the condition and provide a valid next action where the existing contract expects one.
- Monitoring rates that are not reliable remain `n/a` with an explanation; do not render misleading zero rates. Unreachable clusters remain explicitly identified.
- Polling must remain scoped to the active view and must not rebuild live view DOM in ways that lose scroll or focus.

## Architecture and data flow

Keep the current embedded SPA and vanilla JavaScript approach. The shared shell owns navigation, theme state, responsive layout, and page framing. Views remain responsible for their own API calls and controls, using existing helpers/endpoints. Overview composes only data already available through existing contracts; it must show explicit loading/error/unavailable states rather than fabricate metrics. No server, Go, or endpoint changes are part of this design.

## Accessibility and responsive behavior

- Keyboard-operable navigation, view actions, menus, selects, tables, and disclosure controls; visible focus states.
- Semantic labels and accessible names remain available when the sidebar is collapsed.
- Status communicates with text as well as color.
- Dark and light themes maintain readable contrast; no reliance on color alone.
- Layout adapts to narrow screens without horizontal page overflow. Dense tables may use a bounded internal scroller.

## Verification criteria for implementation

- The embedded SPA starts at Overview and every existing view remains reachable.
- Sidebar expanded/collapsed navigation and its mobile form operate by mouse and keyboard.
- Dark/light theme switching and persistence work across navigation and reload.
- Overview metrics/issues/trends map only to existing API fields; loading, unavailable, and error states are accurate.
- Existing Health and Clusters polling, cluster selection/mutations, reliable-rate presentation, and no-flicker behavior remain intact.
- Each migrated developer view retains its existing workflows; no visible control is inert.
- Layout remains usable at desktop and mobile viewport sizes in both themes.

## Out of scope

- Backend endpoints, broker behavior, data model, access-control policy, and new monitoring capabilities.
- Adding third-party frontend frameworks or making the SPA depend on a new external runtime.
- Removing any existing view or feature as a shortcut to the redesign.
