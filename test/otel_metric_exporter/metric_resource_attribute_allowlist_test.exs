defmodule OtelMetricExporter.MetricResourceAttributeAllowlistTest do
  use ExUnit.Case, async: false

  alias OtelMetricExporter.{MetricStore, OtelApi}
  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  @allowlist ~w(service.name deployment.environment.name k8s.cluster.name k8s.node.name)
  @env_resource %{
    "service.name" => "env-service",
    "deployment.environment.name" => "env-environment",
    "k8s.cluster.name" => "env-cluster",
    "k8s.node.name" => "env-node",
    "k8s.pod.uid" => "pod-uid",
    "k8s.pod.name" => "pod-name",
    "k8s.container.name" => "container",
    "k8s.namespace.name" => "namespace",
    "service.version" => "env-version",
    "git.commit.sha" => "commit",
    "image_id" => "image",
    "unknown" => "unknown"
  }
  @app_resource %{
    service: %{name: "app-service", version: "app-version"},
    deployment: %{environment: %{name: "app-environment"}},
    app_only: "app"
  }
  @child_resource %{
    service: %{name: "child-service"},
    k8s: %{node: %{name: "child-node"}},
    child_only: "child"
  }
  @merged_resource Map.merge(@env_resource, %{
                     "service.name" => "child-service",
                     "service.version" => "app-version",
                     "deployment.environment.name" => "app-environment",
                     "k8s.node.name" => "child-node",
                     "app_only" => "app",
                     "child_only" => "child"
                   })
  @env_names ~w(OTEL_RESOURCE_ATTRIBUTES OTEL_SERVICE_NAME OTEL_METRICS_EXPORTER OTEL_LOGS_EXPORTER)

  setup do
    env = Map.new(@env_names, &{&1, System.get_env(&1)})
    app = Application.get_all_env(:otel_metric_exporter)

    on_exit(fn ->
      Enum.each(env, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)

      for {key, _value} <- Application.get_all_env(:otel_metric_exporter),
          do: Application.delete_env(:otel_metric_exporter, key)

      Application.put_all_env(otel_metric_exporter: app)
    end)

    Enum.each(@env_names, &System.delete_env/1)

    for {key, _value} <- app, do: Application.delete_env(:otel_metric_exporter, key)

    System.put_env(
      "OTEL_RESOURCE_ATTRIBUTES",
      Enum.map_join(@env_resource, ",", fn {key, value} -> "#{key}=#{value}" end)
    )

    System.put_env("OTEL_SERVICE_NAME", "env-service-name")
    Application.put_env(:otel_metric_exporter, :resource, @app_resource)
    :ok
  end

  test "child option filters the final exported metric resource and logs retain every attribute" do
    Application.put_env(:otel_metric_exporter, :metric_resource_attribute_allowlist, ["image_id"])
    bypass = Bypass.open()
    parent = self()
    expected = Map.take(@merged_resource, @allowlist)

    Bypass.stub(bypass, "POST", "/v1/metrics", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      request = ExportMetricsServiceRequest.decode(body)
      [resource_metrics] = request.resource_metrics

      resource =
        Map.new(resource_metrics.resource.attributes, fn attr ->
          {attr.key, elem(attr.value.value, 1)}
        end)

      send(parent, {:exported_resource, resource})
      Plug.Conn.resp(conn, 200, "")
    end)

    start_supervised!(
      {OtelMetricExporter,
       name: :resource_allowlist_test,
       metrics: [Telemetry.Metrics.counter("allowlist.event")],
       export_period: :timer.hours(1),
       otlp_endpoint: "http://localhost:#{bypass.port}",
       otlp_compression: nil,
       resource: @child_resource,
       metric_resource_attribute_allowlist: @allowlist}
    )

    assert :sys.get_state(:resource_allowlist_test).api.config.resource == expected
    :telemetry.execute([:allowlist], %{event: 1}, %{})
    assert :ok = MetricStore.export_sync(:resource_allowlist_test)
    assert_receive {:exported_resource, ^expected}

    assert {:ok, logs, _rest} =
             OtelApi.new(api_options(%{metric_resource_attribute_allowlist: @allowlist}), :logs)

    assert logs.config.resource == @merged_resource

    # Resolution must not trim shared configuration used by logs or tracing.
    assert System.get_env("OTEL_SERVICE_NAME") == "env-service-name"

    assert System.get_env("OTEL_RESOURCE_ATTRIBUTES") ==
             Enum.map_join(@env_resource, ",", fn {key, value} -> "#{key}=#{value}" end)

    assert Application.get_env(:otel_metric_exporter, :resource) == @app_resource
  end

  test "omitting the allowlist preserves all merged attributes in metrics and logs" do
    for scope <- [:metrics, :logs] do
      assert {:ok, api, %{}} = OtelApi.new(api_options(), scope)
      assert api.config.resource == @merged_resource
    end
  end

  test "empty allowlist produces an empty metric resource while logs retain all attributes" do
    options = api_options(%{metric_resource_attribute_allowlist: []})
    assert {:ok, metrics, %{}} = OtelApi.new(options, :metrics)
    assert metrics.config.resource == %{}
    assert {:ok, logs, %{}} = OtelApi.new(options, :logs)
    assert logs.config.resource == @merged_resource
  end

  test "application allowlist is used when child option is absent" do
    Application.put_env(:otel_metric_exporter, :metric_resource_attribute_allowlist, @allowlist)
    assert {:ok, metrics, %{}} = OtelApi.new(api_options(), :metrics)
    assert metrics.config.resource == Map.take(@merged_resource, @allowlist)
    assert {:ok, logs, %{}} = OtelApi.new(api_options(), :logs)
    assert logs.config.resource == @merged_resource
  end

  test "explicit allowlists replace the application allowlist including an empty list" do
    Application.put_env(:otel_metric_exporter, :metric_resource_attribute_allowlist, @allowlist)

    for allowlist <- [["child_only", "service.version", "missing"], []] do
      assert {:ok, prepared} =
               MetricStore.prepare_config(
                 api_options(%{metric_resource_attribute_allowlist: allowlist})
               )

      assert prepared.api.config.resource == Map.take(@merged_resource, allowlist)
    end
  end

  test "env service name and resource attributes are filtered even without explicit resources" do
    Application.delete_env(:otel_metric_exporter, :resource)

    assert {:ok, api, %{}} =
             OtelApi.new(
               api_options(%{resource: %{}, metric_resource_attribute_allowlist: @allowlist}),
               :metrics
             )

    assert api.config.resource ==
             @env_resource
             |> Map.put("service.name", "env-service-name")
             |> Map.take(@allowlist)
  end

  test "already-resolved configuration filters metrics without changing logs" do
    options =
      api_options(%{
        resource: @merged_resource,
        exporter: :otlp,
        otlp_endpoint_kind: :generic,
        metric_resource_attribute_allowlist: @allowlist
      })

    assert {:ok, metrics, %{}} = OtelApi.new_effective(options, :metrics)
    assert metrics.config.resource == Map.take(@merged_resource, @allowlist)
    assert {:ok, logs, %{}} = OtelApi.new_effective(options, :logs)
    assert logs.config.resource == @merged_resource
  end

  test "the child option rejects values other than lists of strings" do
    for invalid <- [nil, "service.name", [:"service.name"], [123]] do
      assert {:error, error} =
               OtelMetricExporter.start_link(
                 metrics: [],
                 metric_resource_attribute_allowlist: invalid
               )

      assert error.key == :metric_resource_attribute_allowlist
    end
  end

  defp api_options(extra \\ %{}) do
    Map.merge(
      %{
        finch: OtelMetricExporter.Finch,
        otlp_endpoint: "http://localhost:4318",
        resource: @child_resource
      },
      extra
    )
  end
end
