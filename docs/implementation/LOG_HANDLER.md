# Logger encoding and preparation failures

OtlpUtils owns generic value representation for report values and attributes.
It checks proper tails before enumeration, then classifies the whole list:

| Value | OTLP AnyValue |
|---|---|
| Nonempty whole keyword list | kvlist, preserving order and duplicate keys |
| Empty or other proper list | array, preserving every element |
| Tuple | existing tuple-to-array conversion |
| Improper list | existing inspected-string representation at that boundary |

For example, `%{payload: [{:a, 1}, :b]}` has a payload array containing the tuple
array `["a", 1]` and `"b"`. `%{payload: [[1 | 2]]}` has an array containing the
inspected string `"[1 | 2]"`. Nested arrays, tuples and keyword values follow the
same boundary rule. Mixed atom/string-keyed pairs become arrays rather than
accidental kvlists. Protocol still interprets whole report bodies first; whole
improper report bodies are outside this generic-value correction.

The integration branch's existing boolean, arbitrary-key and nested-struct
support is preserved, along with configuration, disabled exporters, task
completion, graceful shutdown and metric behavior. The proper-tail and whole
keyword scans are linear. No performance improvement is claimed.

## Callback and diagnostic boundaries

LogHandler owns containment: its private preparation helper rescues errors only
from LogAccumulator.prepare_log_event/2. It emits one bounded diagnostic and
returns an error result; the callback then returns :ok without loading that event.
The decision follows control flow, not diagnostic tags.

PID lookup stays outside the outer wrapper. Liveness and :logger_olp.load/2 stay
outside the preparation rescue. The existing outer catch emits a diagnostic and
re-raises the original kind/reason/stack for genuine liveness/load failures and
preparation exits/throws. Disabled handlers remain inert; deliberate removal
retains the integration branch's graceful shutdown behavior.

An escaping callback failure can make OTP remove the handler, stop the OLP and
shut down its temporary significant-child supervisor. This fix does not reinstall
missing handlers. HTTP failure can instead drop a batch while components stay
alive; that is separate from preparation failure. LogAccumulator already owns
completed task cleanup on this integration base, including monitor flushing; no
accumulator change is part of this PR.

Existing LogHandlerFailureTelemetry emits
`[:otel_metric_exporter, :log_handler, :exception]`, measurement `%{count: 1}`,
with exactly six bounded fields: stage, failure_source, exception, message_shape,
trace_context and olp_alive. Its types enumerate allowed values. The optional
internal known :prepare stage preserves five-argument callers and prevents a
truncated stack hiding the known boundary. Source classification remains
approximate. No event, metadata values, reason text/stacks, HTTP response,
endpoint, headers or configuration is included. Its existing process-local
guard suppresses recursive diagnostics and clears after emission. The backend's
six-field allowlist was checked read-only and is compatible.

## Target, evidence and adoption

The user selected `githits-robustness-improvements` as the target on 2026-10-02,
superseding the initial handoff's main target. Base is
`16a7c4da3ad1eae3142ed7d9cc47201bbda58d4c`, containing merged fork PRs #1–#3.
The original main candidate's task fix and imported telemetry helper are already
present here and are not reapplied. Previous main-based verification does not
substitute for this target's verification.

Focused verification on 2026-10-02 used Elixir 1.19.5 and Erlang/OTP 28.4.1
(erts 16.3), with inherited OTEL settings removed and synthetic empty headers:

```text
mix test test/otel_metric_exporter/protocol_test.exs test/otel_metric_exporter/log_handler_failure_telemetry_test.exs test/otel_metric_exporter/log_handler_integration_test.exs test/otel_metric_exporter/log_accumulator_test.exs test/otel_metric_exporter/log_handler_supervisor_test.exs
```

All 72 tests passed; the last two files verify retained accumulator and handler
lifecycle compatibility. Four encoder regressions failed on the uncorrected
integration encoder. `mix format --check-formatted` and diff whitespace checks
passed. The dependency lock is unchanged from the integration base. The revised
implementation review and PR CI are pending.

Receiver tests use normal Logger translation, loopback Bypass and gzip/protobuf
decoding. Both report families must deliver traced and trace-free, followed by
ordinary events through the same installed handler/supervisor/OLP. Invalid trace
metadata must emit one diagnostic, omit the malformed record from every captured
batch and allow subsequent delivery. Fixture-only concurrency1/buffer1/no-retry
and same-producer OLP order provide a later-marker barrier; all unmatched records
and the full matched batch are inspected. No production serialization is added.
Teardown waits for Bypass callback completion and owned supervisor DOWN.

The deep-error fixture uses the test-local Access report struct with recursive
frames. Arbitrary map keys work on this base, so the old main-specific key-error
fixture cannot prove containment. Pure callback tests retain actual load/liveness
failures, preparation exits/throws, reentrancy and explicit prepare-stage proof
when Protocol is absent from the retained stack.

No broad local suite, production failure injection or deployment is authorized.
Normal PR CI is available on this target; tag publication is separate. Other
runtimes are not claimed tested without results. Historical payload remains
unknown; synthetic diagnostic matches do not prove unique incident attribution.
Backend filter repair is merged/deployed per independent handoff; later adoption
needs the reviewed immutable merged exporter SHA and delivery through its filter.
An unmerged candidate or component liveness alone does not close the incident.

Planning had three external rounds, findings incorporated and clean internal
closure but no clean external post-correction check. Main-candidate implementation
round1 had only a low doc finding, corrected under the doc-only rule; it is
superseded by this base correction. Revised implementation review is recorded in
the PR. Keep the temporary plan through review, then transfer durable facts here
and delete the plan in the final implementation commit.
