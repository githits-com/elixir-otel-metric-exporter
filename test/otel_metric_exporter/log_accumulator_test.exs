defmodule OtelMetricExporter.LogAccumulatorTest do
  use ExUnit.Case, async: false

  alias OtelMetricExporter.ExportTelemetry
  alias OtelMetricExporter.LogAccumulator
  alias OtelMetricExporter.LogAccumulator.PendingTask
  alias OtelMetricExporter.OtelApi
  alias OtelMetricExporter.Opentelemetry.Proto.Logs.V1.LogRecord

  @event [:otel_metric_exporter, :export, :stop]

  setup do
    handler_id = {__MODULE__, make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      @event,
      fn event, measurements, metadata, _config ->
        send(parent, {:export_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  describe "handle_info/2" do
    test "ignores unexpected messages without crashing" do
      state = %{pending_tasks: %{}}

      assert {:noreply, ^state} = LogAccumulator.handle_info(:some_unexpected_message, state)
      assert {:noreply, ^state} = LogAccumulator.handle_info({:foo, :bar}, state)
      assert {:noreply, ^state} = LogAccumulator.handle_info({:EXIT, self(), :normal}, state)
    end

    test "ignores task reply with unknown ref without crashing" do
      state = %{pending_tasks: %{}}
      unknown_ref = make_ref()

      assert {:noreply, ^state} =
               LogAccumulator.handle_info({unknown_ref, {:ok, :done}}, state)
    end

    test "removes a known task after its result and flushes its monitor" do
      parent = self()

      task_pid =
        spawn(fn ->
          send(parent, :task_ready)

          receive do
            :stop -> :ok
          end
        end)

      ref = Process.monitor(task_pid)
      task = %Task{mfa: {__MODULE__, :test, []}, owner: self(), pid: task_pid, ref: ref}

      pending_task = %PendingTask{
        task: task,
        batch_size: 1,
        telemetry_start: ExportTelemetry.start()
      }

      state = %{pending_tasks: %{ref => pending_task}}

      assert_receive :task_ready
      on_exit(fn -> send(task_pid, :stop) end)

      assert {:monitors, monitors} = Process.info(self(), :monitors)
      assert {:process, task_pid} in monitors

      assert {:noreply, %{pending_tasks: %{}}} =
               LogAccumulator.handle_info({ref, :ok}, state)

      assert {:monitors, monitors} = Process.info(self(), :monitors)
      refute {:process, task_pid} in monitors

      assert_receive {:export_event, @event, measurements, %{scope: :logs, outcome: :success}}
      assert measurements.batch_size == 1
      assert measurements.rejected_items == 0
      assert measurements.dropped_items == 0
    end

    test "removes a known task after a DOWN message" do
      ref = make_ref()
      task = %Task{mfa: {__MODULE__, :test, []}, owner: self(), pid: self(), ref: ref}

      pending_task = %PendingTask{
        task: task,
        batch_size: 1,
        telemetry_start: ExportTelemetry.start()
      }

      state = %{pending_tasks: %{ref => pending_task}}

      assert {:noreply, %{pending_tasks: %{}}} =
               LogAccumulator.handle_info({:DOWN, ref, :process, self(), :normal}, state)

      assert_receive {:export_event, @event, measurements,
                      %{scope: :logs, outcome: :retryable_export_task_failed}}

      assert measurements.batch_size == 1
      assert measurements.rejected_items == 0
      assert measurements.dropped_items == 1
    end

    test "ignores DOWN message with unknown ref without crashing" do
      state = %{pending_tasks: %{}}
      unknown_ref = make_ref()

      assert {:noreply, ^state} =
               LogAccumulator.handle_info(
                 {:DOWN, unknown_ref, :process, self(), :normal},
                 state
               )
    end

    test "ignores late task messages after the task was completed" do
      ref = make_ref()
      state = %{pending_tasks: %{}}

      assert {:noreply, ^state} =
               LogAccumulator.handle_info({ref, {:ok, :done}}, state)

      assert {:noreply, ^state} =
               LogAccumulator.handle_info({:DOWN, ref, :process, self(), :normal}, state)
    end

    test "classifies partial and final errors and emits once" do
      for {result, outcome, dropped_items} <- [
            {{:partial_success, 2}, :partial_success, 0},
            {{:error, :terminal, :invalid_response}, :terminal_invalid_response, 2},
            {{:error, :retryable, :transport_failure}, :retryable_transport_failure, 2}
          ] do
        ref = make_ref()
        task = %Task{mfa: {__MODULE__, :test, []}, owner: self(), pid: self(), ref: ref}

        state = %{
          pending_tasks: %{
            ref => %PendingTask{
              task: task,
              batch_size: 2,
              telemetry_start: ExportTelemetry.start()
            }
          }
        }

        assert {:noreply, %{pending_tasks: %{}}} =
                 LogAccumulator.handle_info({ref, result}, state)

        assert_receive {:export_event, @event, measurements, %{scope: :logs, outcome: ^outcome}}
        assert measurements.batch_size == 2
        assert measurements.rejected_items == if(outcome == :partial_success, do: 2, else: 0)
        assert measurements.dropped_items == dropped_items

        assert {:noreply, %{pending_tasks: %{}}} =
                 LogAccumulator.handle_info({ref, result}, %{pending_tasks: %{}})

        refute_receive {:export_event, @event, _, _}, 20
      end
    end

    test "drops pending tasks and queued logs once when shutdown reaches its deadline" do
      task_pid = spawn(fn -> receive do: (:stop -> :ok) end)
      ref = Process.monitor(task_pid)
      task = %Task{mfa: {__MODULE__, :test, []}, owner: self(), pid: task_pid, ref: ref}

      state = %{
        api: test_api(0),
        event_queue: [%LogRecord{}],
        queue_len: 1,
        queue_telemetry_start: ExportTelemetry.start(),
        pending_tasks: %{
          ref => %PendingTask{
            task: task,
            batch_size: 2,
            telemetry_start: ExportTelemetry.start()
          }
        }
      }

      assert :ok = LogAccumulator.terminate(:shutdown, state)

      assert_receive {:export_event, @event, %{batch_size: 2, dropped_items: 2},
                      %{scope: :logs, outcome: :retryable_deadline_exceeded}}

      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 1},
                      %{scope: :logs, outcome: :retryable_deadline_exceeded}}

      refute_receive {:export_event, @event, _, _}, 20
    end

    test "consumes an already-delivered result before classifying shutdown termination" do
      task_pid = spawn(fn -> receive do: (:stop -> :ok) end)
      ref = Process.monitor(task_pid)
      task = %Task{mfa: {__MODULE__, :test, []}, owner: self(), pid: task_pid, ref: ref}

      state = %{
        api: test_api(0),
        event_queue: [],
        queue_len: 0,
        queue_telemetry_start: nil,
        pending_tasks: %{
          ref => %PendingTask{
            task: task,
            batch_size: 2,
            telemetry_start: ExportTelemetry.start()
          }
        }
      }

      send(self(), {ref, :ok})
      assert :ok = LogAccumulator.terminate(:shutdown, state)

      assert_receive {:export_event, @event, measurements, %{scope: :logs, outcome: :success}}
      assert measurements.batch_size == 2
      assert measurements.rejected_items == 0
      assert measurements.dropped_items == 0
      refute_receive {:export_event, @event, _, _}, 20
    end

    test "does not emit for an empty shutdown queue" do
      state = %{
        api: test_api(0),
        event_queue: [],
        queue_len: 0,
        queue_telemetry_start: nil,
        pending_tasks: %{}
      }

      assert :ok = LogAccumulator.terminate(:shutdown, state)
      refute_receive {:export_event, @event, _, _}, 20
    end
  end

  defp test_api(timeout) do
    {:ok, api, %{}} =
      OtelApi.new(
        %{
          finch: :test_finch,
          otlp_endpoint: "http://localhost:4318",
          otlp_timeout: max(timeout, 1)
        },
        :logs
      )

    %{api | config: %{api.config | otlp_timeout: timeout}}
  end
end
