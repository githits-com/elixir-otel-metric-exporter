# Logger encoding and preparation containment on the integration branch

## Authority, outcome and current state

Status: implementation, verification and review complete on 2026-10-02; final
doc closure and draft PR delivery underway.
User explicitly selected `githits-robustness-improvements`, superseding the
handoff's `main` target. PR #4 remains an own-fork draft; no merge, tag, release,
publish, deployment or other-lane mutation is authorized.

Target: `githits-com/elixir-otel-metric-exporter`,
`origin/githits-robustness-improvements` =
`16a7c4da3ad1eae3142ed7d9cc47201bbda58d4c`. Fetch verified PRs #1–#3 are merged
there, while main stays d579d0f and excludes them. Original main-based candidate
b905d9f was verified/reviewed but its evidence does not substitute for testing
this rebased target. Retain its commit and review history; do not import main's
older implementations over the fork's merged changes.

Outcome: both label-free report maps `%{payload: [{:a, 1}, :b]}` and
`%{payload: [[1 | 2]]}` export intact; an error preparing one event omits only
that event, emits one safe diagnostic, and later valid events arrive through the
same handler, supervisor and OLP. Exits/throws and genuine liveness/load failures
retain original failure semantics. Historical payload remains unknown; matching
synthetic diagnostic tags do not establish unique attribution.

Verified target differences:

- Telemetry helper and outer callback wrapper already exist. Add only narrow
  preparation rescue and optional known prepare stage; no helper import.
- Boolean precedence, nested structs and arbitrary term keys are already fixed.
  Preserve those report and configuration contracts and existing tests.
- Accumulator completion already removes pending state and demonitor-flushes
  through its complete_task helper in both reply paths. Preserve it unchanged;
  remove the main-only task fix and duplicate synthetic task tests from this PR.
- Protocol report-body interpretation is unchanged. Whole improper report bodies
  remain outside the generic value correction.
- The old map-key deep-error fixture is invalid here because arbitrary keys work.
  Use the existing test-local Access report struct to raise ArgumentError through
  sixteen non-tail recursive frames; prove Protocol is absent from retained stack
  before testing containment/knownstage. Throws/exits use the same fixture.
- Base has PR CI (format and test) as well as tag publication. Permit ordinary PR
  CI; never invoke publication. No broad local suite is authorized.
- Base dependency lock differs from main. Fetch its locked dependencies without
  changing versions; verify mix.lock remains exactly the target's version.

## Ownership, scope and acceptance

OtlpUtils owns shared generic-value classification; check proper tail before
Enum, preserve nonempty whole keywords/order/duplicates, empty arrays and tuple
conversion, use inspected strings at improper value boundaries. LogHandler owns
containment because its preparation/load boundary is explicit. Existing telemetry
helper owns six-field bounded classification and reentrancy. No new placement,
infrastructure, dependency, recovery path or producer constraint is needed.

Acceptance retained from approved planning:

- Pure matrix: proper mixed/atom-string-keyed lists retain all elements as arrays;
  keyword order/duplicates and nested values remain; improper values directly or
  within arrays/tuples/keywords use strings; protobuf report-map round trip works.
- Real Logger and normal translator deliver both reports traced and trace-free,
  then unique ordinary events; same handler, supervisor and OLP remain installed.
- Actual Logger invalid trace ID not-hex yields count1 with exactly the six bounded
  fields, callback :ok/no load, omission from every captured receiver record and
  subsequent valid delivery. Check unmatched records and complete matched batch.
- Deep actual preparation error still gets explicit stage prepare with Protocol
  absent from stack. No inference determines containment. Genuine liveness/load
  failures and preparation exits/throws preserve original kind/reason/stack.
- Reentrant bad event is contained with no recursive diagnostic; guard clears.
- Existing arbitrary-key/struct/bool, disabled handler, configuration and task
  lifecycle behavior are preserved rather than rewritten from main.

## Ordered work and phases

Phase 1: backend filter repair already merged/deployed per independent handoff;
read-only context, no operations here. Phase 2: this upstream PR adaptation and
verified delivery, complete. Phase 3: later backend adoption through its filter,
owned by that lane and awaiting a reviewed immutable merged exporter SHA.

Phase 2 assumptions: verified current integration base and existing fixture order.
Product decisions/unknowns: none. Dependencies: existing locked packages, Bypass,
protobuf and telemetry. Later phase unknown: exact approved merge SHA.

1. Rebase own PR branch onto verified integration base; preserve all unrelated
   files. Resolve overlapping files by retaining target behavior and applying only
   required encoder/rescue/knownstage/test deltas. Keep README and CHANGELOG
   consistent with existing merged contracts, avoiding duplicate telemetry docs.
2. Establish pure encoder baseline on this target, then apply the classification
   fix. Adapt contained-error and deep Access tests without build/support changes.
3. Run focused protocol, LogHandlerFailureTelemetry and Logger integration files
   together, plus retained accumulator/supervisor tests only where needed to prove
   base-specific callback lifecycle compatibility. No broad local suite.
4. Format changed source/tests and satisfy PR CI. No benchmarks: correctness only,
   linear classification scans, no optimization claim. Record actual versions.
5. Internal full-delta preflight, then retained Claude reviewer round2 of this PR
   (round1 reviewed the superseded main candidate). Revised direction/base context
   must be assessed; no additional planning review is needed because scope,
   ownership, phase boundaries and acceptance are unchanged.
6. Fix accepted findings, rerun affected proof, commit stable points. Preserve
   previous planning review truth: three rounds, findings incorporated and clean
   internal closure, no clean external post-correction planning check.
7. After clean required implementation review, transfer durable knowledge and any
   major deferred items to permanent docs and remove this plan in the final commit.
   Force-push own branch with lease and retarget/update the same draft PR.

Security: secret-free test environment, explicit synthetic empty headers and
loopback endpoint; never print resolved Logger options or raw callback crash
reports. Fixture-only request limit1/buffer1/no-retry and same-producer OLP order
provide the omission barrier without adding production serialization. Await
receiver/BYPASS completion and owned supervisor monitors, never sleep/poll fixes.
Rollback: ordinary branch/pin revert; no migrations or cleanup. No new major
issue deferred. Boundary refactoring already uses private classification and
preparation helpers; no broader refactor is justified.

## Evidence and review

- Actual toolchain: Elixir1.19.5, Erlang/OTP28.4.1, erts16.3 (the base's
  .tool-versions selects Elixir1.19.5/OTP28.4). Other runtimes not claimed tested.
- mix deps.get fetched the integration lock's existing versions; mix.lock is
  unchanged. The first attempt failed because the previous Hex archive used
  a newer Elixir intrinsic; compatible Hex setup resolved it.
- Pure encoder baseline with target OtlpUtils restored temporarily: four named
  mixed/improper/roundtrip cases failed; candidate file restored unconditionally.
- First and second focused runs:72 tests,1 failure, solely the deep fixture's
  retained-stack assertion. The private never-return recursion lost frames;
  public test-local entry plus alternating non-tail call sites preserves them.
  Ownership remains test-local; no production machinery/build change introduced.
- Narrow failure telemetry file after fixture correction:12 tests,0 failures.
- Final command:mix test test/otel_metric_exporter/protocol_test.exs
  test/otel_metric_exporter/log_handler_failure_telemetry_test.exs
  test/otel_metric_exporter/log_handler_integration_test.exs
  test/otel_metric_exporter/log_accumulator_test.exs
  test/otel_metric_exporter/log_handler_supervisor_test.exs
  ->72 tests,0 failures,seed759457,3.1s. The last two retained suites verify the
  inherited accumulator completion and handler lifecycle/configuration behavior.
- mix format --check-formatted passed. Accumulator source/tests and mix.lock are
  exactly the integration base. Diff whitespace checks passed.
- Internal full-delta implementation preflight on the selected integration base:
  direction sound, no findings. Production differences, retained base contracts,
  deep fixture, actual receiver proof and documentation checked read-only.
- External Claude Opus5.5 round2 on the integration delta e6f2b4d: fresh direction
  check sound, no code findings, single fresh-context final check also no code
  findings. Two doc nits accepted: D1 inconsistent blank line in CHANGELOG list
  -> scanned entire Unreleased list -> removed separator; D2 pending/process
  instructions would go stale -> scanned complete permanent notes/README/plan
  -> replaced pending statements with actual review/CI outcomes and moved review
  process history into PR. These doc-only notes are applied; round clean under
  user's rule, no third round or post-doc external recheck claimed. Reviewer
  retained for merge approval.
- Fresh-check residual hypothetical emit failure/double-emission is not a
  finding: telemetry isolates consumer failures and classifiers are total for
  Logger events; no verified trigger, no new guard warranted. Do not re-raise
  without new evidence. Prior settled whole-report-body, raw reason and fixture
  consolidation notes remain closed.
- CI run36999675154 on e6f2b4d: Check formatting passed; Run tests passed with
  212 tests,0 failures on setup-beam OTP28.4 / Elixir1.19.5, erts16.3.
  https://github.com/githits-com/elixir-otel-metric-exporter/actions/runs/36999675154
- Actual receiver, contained-error, genuine-failure and inherited compatibility
  outcomes verified. No outstanding scoped finding or major deferred item.
  Remaining backend adoption is explicitly a later independent lane.
- Durable contracts/evidence transferred to docs/implementation/LOG_HANDLER.md
  and README/CHANGELOG; remove this completed plan as final PR commit.
- Own-fork draft PR#4 already retargeted and updated; final docs/plan cleanup
  push pending. No merge/release/deployment or other-lane change performed.
