# Universal API alpha.19 projection audit

The module pins `github.com/MontFerret/api v1.0.0-alpha.19`. The published tag and
matching API checkout resolve to `fa6c2057aef6be7f9980dcd3d8e498b66d14ecd6`.
This matches the sibling Ferret dependency. Wire remains runtime independent;
native Ferret round-trip validation is a separate task.

The six interfaces below are audited against that checkout. Production
compile-time assertions cover all six. The client returns `api.Runtime` and
borrows caller transport; the server borrows the hosted runtime. Public spy
integration tests use real gRPC and no native runtime dependency.

| Interface | Method/setter | Projection and retained behavior | Coverage |
| --- | --- | --- | --- |
| `api.Runtime` | `Run(ctx, Source, ...SessionOption) (*Output, error)` | One hosted Run; optional output independent of execution/cleanup errors; temporary Execution released once. | `TestRuntimeAndSessionOutputRoundTrip`, `TestOutputPresenceAndCleanupErrors` |
| | `Compile`, `CompileDebug` | Corresponding hosted method, eager copied fallible metadata, unpublished plan cleanup. Anonymous sources preserved. | `TestCompileRoundTrip`, `TestMetadataFailureClosesUnpublishedPlan`, `TestOptionPresenceAndAnonymousSources` |
| | `Close` | Reject new root work; admitted work/descendants survive; last reference releases connection. Result stable; transport and host borrowed. | `TestRuntimeCloseBorrowsTransportAndHostedRuntime`, `TestDeferredRuntimeCloseRetainsItsResult`, `TestRuntimeClosePreservesAdmittedCompile` |
| `api.Plan` | `Params() ([]string, error)` | Defensive copy of captured metadata; no lazy hosted request. | `TestCompileRoundTrip`, metadata failure test above |
| | `NewSession`, `NewDebugSession` | Direct constructors; ordered options; admitted constructors settle before ordinary plan closure. | `TestReusablePlanAndDurableSessions`, `TestDebuggerRoundTrip`, core `TestPlanCloseSettlesAdmittedConstructorWithoutCancellingChildren` |
| | `Close` | New ClosePlan RPC closes only plan; published children survive; transport retained until last child. | `TestParentClosePreservesChildrenAndActiveWork`, core constructor test above |
| `api.Session` | `Run(ctx) (*Output, error)` | Same durable hosted session, sequential execution; optional output and joined cleanup error. | `TestOutputPresenceAndCleanupErrors`, `TestSessionRejectsOverlapAndReopensAfterRelease` |
| | `Close` | Settle active execution, release hosted session once, retain result. | `TestParentClosePreservesChildrenAndActiveWork`, `TestKnownResourceCloseFailurePreservesSiblings` |
| `debugger.Session` | `Start(ctx)` | RunCommand stream retains Start lifetime after initial result. | `TestDebuggerRoundTrip`, `TestStartContextSurvivesInitialStop` |
| | `Continue(ctx)`, `StepIn(ctx)`, `StepOver(ctx)`, `StepOut(ctx)` | Individual request contexts; independent optional event and command error. | `TestDebuggerRoundTrip`, `TestCancellationReachesHostedOperations`, `TestDebuggerCommandEventAndError` |
| | `Pause(ctx)` | Direct hosted pause; canceled requests never ask for a stop. | `TestDebuggerRoundTrip`, `TestDebuggerContextsAndDirectOperations` |
| | `SetBreakpoint(ctx, Location)` | Options absent; invokes default operation. | `TestDebuggerContextsAndDirectOperations` |
| | `SetBreakpointAt(ctx, Location, Options)` | Options present even at zero; portable metadata and signed function ID preserved. | `TestDebuggerRoundTrip` |
| | `ReplaceBreakpoints(ctx, source, []BreakpointRequest)` | One atomic call, including running; request order, default source, duplicates, unresolved entries, IDs, and metadata preserved; net cross-source quota. | `TestDebuggerReplacementAndRetainedEnumeration` |
| | `DeleteBreakpoint(ctx, ID)` | Hosted deletion with context; ID lookup scoped to debugger. | `TestDebuggerRoundTrip`, `TestErrorFamilies` |
| | `Breakpoints(ctx)` | Hosted enumeration while open; detached final data/error retained after explicit Close. | `TestDebuggerReplacementAndRetainedEnumeration`, `TestDebuggerEnumerationFailureAfterClose` |
| | `Frames(ctx)` | Order gives frame index; signed function ID includes -1. | `TestDebuggerRoundTrip`, replacement test above |
| | `Locals(ctx)`, `FrameLocals(ctx, index)` | Distinct default/indexed calls. | `TestDebuggerRoundTrip`, `TestDebuggerContextsAndDirectOperations` |
| | `Variables(ctx, reference)` | Stopped-state references; stale/invalid references remain errors. | `TestDebuggerRoundTrip` |
| | `Evaluate(ctx, expression)`, `EvaluateFrame(ctx, index, expression)` | Distinct direct hosted operations, forwarding cancellation/deadlines. | `TestDebuggerRoundTrip`, `TestCancellationReachesHostedOperations`, context test above |
| | `Close` | Cancel/settle commands, close hosted debugger once, capture final enumeration, release handle; retain close error. | `TestParentClosePreservesChildrenAndActiveWork`, `TestDebuggerEnumerationFailureAfterClose` |
| `api.PlanOptions` | `SetOptimizationLevel` | Explicit zero versus absence; valid enum check; ordered overrides. | `TestCompileRoundTrip`, `TestCompileOptionsApplyOnceBeforeDispatch` |
| `api.SessionOptions` | `SetParam`, `SetParams` | Ordered single override and map merge; snapshot portable values. | `TestSessionOptionsRoundTrip` |
| | `SetOutputContentType` | Preserve omitted/nonempty/explicit empty; host validates codec. | `TestOptionPresenceAndAnonymousSources`, protocol `TestProtocolResourceOperationsRemainAvailable` |
| | `SetFSRoot` | Preserve presence and verbatim value on runtime, temporary Execute, durable Session, and debugger creation. Host owns validation. | `TestOptionPresenceAndAnonymousSources`, protocol test above |

All debugger context-taking methods reject nil and canceled contexts before
dispatch; `TestDebuggerContextsAndDirectOperations` covers the complete method
set. Server admission rechecks cancellation. Normal API closure does not bypass
transport release gates. Existing disconnect, allocation-loss escalation, quota,
panic containment, stale-ID, watcher, and release-error suites remain in place.

Protocol additions are listed in [Wire Protocol](protocol.md#alpha19-additive-contract).
The [integration coverage table](../test/integration/README.md) maps the wider
lifecycle and security coverage. Runtime semantics, path interpretation, source
validity, debugger binding, atomic matching, and codec implementation stay hosted.

Baseline and post-change measurements are recorded in [Migration benchmarks](uapi-benchmarks.md).
