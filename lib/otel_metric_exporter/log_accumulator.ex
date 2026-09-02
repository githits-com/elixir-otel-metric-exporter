defmodule OtelMetricExporter.LogAccumulator do
  @moduledoc false
  alias OtelMetricExporter.Opentelemetry.Proto.Logs.V1.LogRecord
  alias OtelMetricExporter.OtelApi
  # This module is used as a callback for the logger_olp module.
  # `:logger_olp` expects certain callbacks, but doesn't define an explicit
  # behaviour for them, so we're using a GenServer behaviour as an approximation.
  #
  # On top of regular GenServer callbacks, `:logger_olp` expects:
  # - `handle_load(event, state) :: state` - required, this is a new log record
  # - `reset_state(state) :: state` - optional
  # - `notify(handler_event, state) :: state` - optional
  #
  # Handler events are defined in `:logger_olp` as:
  # ```elixir
  #   handler_event ::
  #     :idle
  #     | :restart
  #     | {:flushed, flushed :: non_neg_integer}
  #     | {:mode_change, old_mode :: mode, new_mode :: mode}
  #   mode :: :drop | :sync | :async
  # ```
  #
  # `logger_olp` will switch between sync and async modes depending on the message
  # queue length. Sync mode means the process that tries to send the logs will block
  # until the logs are sent. Async mode means the process will return immediately and
  # send the logs in the background. When this module is used as a callback,
  # `logger_olp` should be configured to never change into sync mode, as that will block
  # one of the processes emitting the log until an HTTP request is resolved, which
  # is not what we want.

  @behaviour GenServer

  alias OtelMetricExporter.ExportTelemetry

  defmodule PendingTask do
    @moduledoc false

    defstruct [:task, :batch_size, :telemetry_start]

    @type t :: %__MODULE__{
            task: Task.t(),
            batch_size: non_neg_integer(),
            telemetry_start: ExportTelemetry.start_time()
          }
  end

  @schema NimbleOptions.new!(
            metadata: [
              type: {:list, :atom},
              default: [],
              doc: "A list of atoms from metadata to attach as attribute to the log event"
            ],
            metadata_map: [
              type: {:map, :atom, :string},
              default: %{},
              doc: """
              Remapping of metadata keys to different attribute names.
              Example: Plug adds a `request_id` metadata key to log events, but
              semantic convention for OTel is to use `http.request.id`. This can be
              achieved by specifying this field to `%{request_id: "http.request.id"}`
              """
            ],
            debounce_ms: [
              type: :non_neg_integer,
              default: 1_000,
              doc: "Period to accumulate logs before sending them"
            ],
            max_buffer_size: [
              type: :non_neg_integer,
              default: 5_000,
              doc: "Max amount of log events to store before sending them"
            ]
          )

  @doc false
  def options_schema(), do: @schema

  defdelegate prepare_log_event(event, config), to: OtelMetricExporter.Protocol

  def check_config(config, base_name) do
    config =
      Map.new(config)
      |> Map.put(:finch, :"#{base_name}_Finch")

    with {:ok, api, rest} <- OtelApi.new(config, :logs),
         {:ok, validated} <- NimbleOptions.validate(rest, @schema) do
      {:ok,
       Map.put(validated, :api, api) |> Map.put(:task_supervisor, :"#{base_name}_TaskSupervisor")}
    end
  end

  def init(%{api: %{config: %{exporter: :none}}}), do: :ignore

  def init(config) do
    Process.flag(:trap_exit, true)

    {:ok,
     Map.merge(config, %{
       event_queue: [],
       queue_len: 0,
       queue_telemetry_start: nil,
       timer_ref: nil,
       pending_tasks: %{}
     })}
  end

  def handle_load(event, state) do
    state
    |> add_event(event)
    |> send_schedule_or_block()
  end

  def handle_call({:config_changed, config}, _from, state) do
    {:reply, :ok, Map.merge(state, config, &if(&1 == :api, do: Map.merge(&2, &3), else: &3))}
  end

  def handle_info(:send_log_batch, state) do
    # Reset the timer
    state = Map.put(state, :timer_ref, nil)

    case state do
      %{event_queue: []} ->
        # Do nothing on an empty queue
        {:noreply, state}

      %{pending_tasks: pending_tasks, api: api}
      when map_size(pending_tasks) < api.config.otlp_concurrent_requests ->
        # We have some task budget, let's put in a task
        {:noreply, send_events_via_task(state)}

      _ ->
        # No task budget, so we're blocking on this message
        {:noreply, state |> block_until_any_task_ready() |> send_events_via_task()}
    end
  end

  def handle_info({ref, result}, state)
      when is_map_key(state.pending_tasks, ref) do
    {:noreply, complete_task(state, ref, {:result, result})}
  end

  def handle_info({:DOWN, ref, :process, _, _}, state)
      when is_map_key(state.pending_tasks, ref) do
    {:noreply, complete_task(state, ref, :exit)}
  end

  # Ignore stray messages without feeding diagnostics back into this logger handler.
  def handle_info(_msg, state), do: {:noreply, state}

  def terminate(_reason, state) do
    deadline = OtelApi.new_deadline(state.api)
    shutdown(state, deadline)
    :ok
  end

  # We don't care about event order# If we have a here, because it's all timestamped
  # and up to the log display to order
  defp add_event(%{event_queue: queue, queue_len: len} = state, event)
       when is_struct(event, LogRecord) do
    state
    |> Map.put(:event_queue, [event | queue])
    |> Map.put(:queue_len, len + 1)
    |> maybe_start_queue_telemetry(len)
  end

  defp maybe_start_queue_telemetry(state, 0),
    do: Map.put(state, :queue_telemetry_start, ExportTelemetry.start())

  defp maybe_start_queue_telemetry(state, _len), do: state

  # If we have the maximum number of concurrent requests, block
  defp send_schedule_or_block(%{pending_tasks: pending_tasks} = state)
       when map_size(pending_tasks) == state.api.config.otlp_concurrent_requests do
    state
    |> block_until_any_task_ready()
    |> send_schedule_or_block()
  end

  # If we have enough events to send, send immediately
  defp send_schedule_or_block(%{queue_len: len} = state)
       when len >= state.max_buffer_size do
    # Send the logs immediately
    send_events_via_task(state)
  end

  # If we have a schedule already, do nothing
  defp send_schedule_or_block(%{timer_ref: ref} = state) when not is_nil(ref), do: state

  defp send_schedule_or_block(%{debounce_ms: debounce_ms} = state) do
    timer_ref = Process.send_after(self(), :send_log_batch, debounce_ms)

    %{state | timer_ref: timer_ref}
  end

  defp block_until_any_task_ready(%{pending_tasks: pending_tasks} = state) do
    # Block via a receive, waiting for a completion message or a down message
    # from a task that we started
    receive do
      {ref, result} when is_map_key(pending_tasks, ref) ->
        complete_task(state, ref, {:result, result})

      {:DOWN, ref, :process, _, _} when is_map_key(pending_tasks, ref) ->
        complete_task(state, ref, :exit)
    end
  end

  defp complete_task(%{pending_tasks: pending_tasks} = state, ref, completion) do
    {pending_task, pending_tasks} = Map.pop!(pending_tasks, ref)

    %PendingTask{
      batch_size: batch_size,
      telemetry_start: telemetry_start
    } = pending_task

    Process.demonitor(ref, [:flush])
    {result, dropped_items} = final_log_result(completion, batch_size)
    ExportTelemetry.stop(telemetry_start, :logs, batch_size, result, dropped_items)
    %{state | pending_tasks: pending_tasks}
  end

  @spec final_log_result(
          {:result, OtelApi.export_result()} | :exit | :deadline,
          non_neg_integer()
        ) ::
          {OtelApi.export_result(), non_neg_integer()}
  defp final_log_result({:result, :ok}, _batch_size), do: {:ok, 0}

  defp final_log_result({:result, {:partial_success, rejected_items}}, _batch_size),
    do: {{:partial_success, rejected_items}, 0}

  defp final_log_result({:result, {:error, disposition, reason}}, batch_size),
    do: {{:error, disposition, reason}, batch_size}

  defp final_log_result(:exit, batch_size),
    do: {{:error, :retryable, :export_task_failed}, batch_size}

  defp final_log_result(:deadline, batch_size),
    do: {{:error, :retryable, :deadline_exceeded}, batch_size}

  # Normal exports create their deadline inside the task so scheduler delay does not consume
  # the budget; shutdown passes the deadline created before dispatch.
  defp send_events_via_task(%{api: api, event_queue: queue} = state) do
    batch_size = state.queue_len
    telemetry_start = ExportTelemetry.start()

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, OtelApi, :send_log_events, [
        api,
        queue
      ])

    %{
      state
      | event_queue: [],
        queue_len: 0,
        queue_telemetry_start: nil,
        pending_tasks:
          Map.put(
            state.pending_tasks,
            task.ref,
            %PendingTask{
              task: task,
              batch_size: batch_size,
              telemetry_start: telemetry_start
            }
          )
    }
  end

  defp send_events_via_task(%{api: api, event_queue: queue} = state, deadline) do
    batch_size = state.queue_len
    telemetry_start = ExportTelemetry.start()

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, OtelApi, :send_log_events, [
        api,
        queue,
        deadline
      ])

    %{
      state
      | event_queue: [],
        queue_len: 0,
        queue_telemetry_start: nil,
        pending_tasks:
          Map.put(
            state.pending_tasks,
            task.ref,
            %PendingTask{
              task: task,
              batch_size: batch_size,
              telemetry_start: telemetry_start
            }
          )
    }
  end

  defp shutdown(state, deadline) do
    case OtelApi.remaining_timeout(deadline) do
      :expired ->
        state |> terminate_pending_tasks() |> drop_queued_batch()

      _remaining ->
        cond do
          state.event_queue == [] and map_size(state.pending_tasks) == 0 ->
            :ok

          state.event_queue != [] and
              map_size(state.pending_tasks) < state.api.config.otlp_concurrent_requests ->
            shutdown(send_events_via_task(state, deadline), deadline)

          true ->
            shutdown(await_task(state, deadline), deadline)
        end
    end
  end

  defp await_task(%{pending_tasks: pending_tasks} = state, deadline) do
    case OtelApi.remaining_timeout(deadline) do
      timeout when is_integer(timeout) ->
        receive do
          {ref, result} when is_map_key(pending_tasks, ref) ->
            complete_task(state, ref, {:result, result})

          {:DOWN, ref, :process, _, _} when is_map_key(pending_tasks, ref) ->
            complete_task(state, ref, :exit)
        after
          timeout ->
            state |> terminate_pending_tasks() |> drop_queued_batch()
        end

      :expired ->
        state |> terminate_pending_tasks() |> drop_queued_batch()
    end
  end

  defp terminate_pending_tasks(%{pending_tasks: pending_tasks} = state) do
    Enum.reduce(pending_tasks, state, fn {ref, %PendingTask{task: %Task{pid: pid}}}, state ->
      Process.exit(pid, :kill)

      receive do
        {^ref, result} ->
          complete_task(state, ref, {:result, result})

        {:DOWN, ^ref, :process, ^pid, _reason} ->
          complete_task(state, ref, :deadline)
      end
    end)
  end

  defp drop_queued_batch(%{queue_len: 0} = state), do: state

  defp drop_queued_batch(state) do
    telemetry_start = state.queue_telemetry_start

    ExportTelemetry.stop(
      telemetry_start,
      :logs,
      state.queue_len,
      {:error, :retryable, :deadline_exceeded},
      state.queue_len
    )

    %{state | event_queue: [], queue_len: 0, queue_telemetry_start: nil}
  end
end
