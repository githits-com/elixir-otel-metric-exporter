defmodule OtelMetricExporter.LogAccumulatorTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.LogAccumulator

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
      state = %{pending_tasks: %{ref => :pending}}

      assert_receive :task_ready
      on_exit(fn -> send(task_pid, :stop) end)

      assert {:monitors, monitors} = Process.info(self(), :monitors)
      assert {:process, task_pid} in monitors

      assert {:noreply, %{pending_tasks: %{}}} =
               LogAccumulator.handle_info({ref, {:ok, :done}}, state)

      assert {:monitors, monitors} = Process.info(self(), :monitors)
      refute {:process, task_pid} in monitors
    end

    test "removes a known task after a DOWN message" do
      ref = make_ref()
      state = %{pending_tasks: %{ref => :pending}}

      assert {:noreply, %{pending_tasks: %{}}} =
               LogAccumulator.handle_info({:DOWN, ref, :process, self(), :normal}, state)
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
  end
end
