defmodule OtelMetricExporter.LogHandlerSupervisor do
  @moduledoc false
  use Supervisor, restart: :temporary

  alias OtelMetricExporter.OtelApi

  @otp_cleanup_grace 1_000

  @type accumulator_config :: %{
          required(:api) => %OtelApi{
            config: %OtelMetricExporter.OtelApi.Config{
              otlp_timeout: pos_integer()
            }
          },
          optional(atom()) => term()
        }

  @doc """
  Returns the OTP stop/child allowance: the configured OTLP export timeout plus
  fixed cleanup grace. The grace applies only to OTP termination and does not
  enlarge the export deadline.
  """
  @spec shutdown_timeout(accumulator_config()) :: pos_integer()
  def shutdown_timeout(%{api: %{config: %{otlp_timeout: timeout}}}),
    do: timeout + @otp_cleanup_grace

  def fill_accumulator_config(accumulator_config, base_name) do
    accumulator_config
    |> Map.put(:finch, :"#{base_name}_Finch")
    |> Map.put(:task_supervisor, :"#{base_name}_TaskSupervisor")
  end

  def start_link(args) do
    base_name = args[:name]

    accumulator_config =
      args[:accumulator_config]

    olp_child_spec =
      logger_olp_child_spec(
        :"#{base_name}_logger_olp",
        accumulator_config,
        args[:olp_config]
      )

    with {:ok, sup_pid} <- Supervisor.start_link(__MODULE__, accumulator_config, name: base_name),
         {:ok, _, olp} <- Supervisor.start_child(sup_pid, olp_child_spec) do
      {:ok, sup_pid, olp}
    else
      {:error, {reason, child}} when is_tuple(child) and elem(child, 0) == :child ->
        {:error, reason}

      error ->
        error
    end
  end

  @spec child_spec(map() | keyword()) :: Supervisor.child_spec()
  def child_spec(args) do
    args_map = Map.new(args)

    super(args_map)
    |> Map.put(:start, {__MODULE__, :start_link, [args]})
    |> Map.put(:shutdown, shutdown_timeout(args_map.accumulator_config) + @otp_cleanup_grace)
  end

  defp logger_olp_child_spec(reg_name, accumulator_config, olp_config) do
    %{
      id: :logger_olp,
      start:
        {:logger_olp, :start_link,
         [
           reg_name,
           OtelMetricExporter.LogAccumulator,
           accumulator_config,
           olp_config
         ]},
      restart: :temporary,
      significant: true,
      shutdown: shutdown_timeout(accumulator_config),
      type: :worker,
      modules: [OtelMetricExporter.LogAccumulator]
    }
  end

  @impl true
  def init(accumulator_config) do
    children = [
      {Finch,
       name: accumulator_config.api.finch,
       pools: %{
         :default => [size: accumulator_config.api.config.otlp_concurrent_requests, count: 1]
       }},
      {Task.Supervisor, name: accumulator_config.task_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one, auto_shutdown: :any_significant)
  end
end
