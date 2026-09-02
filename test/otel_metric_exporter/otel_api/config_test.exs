defmodule OtelMetricExporter.OtelApi.ConfigTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @touched_envs ~w|OTEL_EXPORTER_OTLP_ENDPOINT OTEL_EXPORTER_OTLP_PROTOCOL OTEL_EXPORTER_OTLP_HEADERS OTEL_EXPORTER_OTLP_TIMEOUT| ++
                  ~w|OTEL_RESOURCE_ATTRIBUTES OTEL_SERVICE_NAME| ++
                  ~w|OTEL_EXPORTER_OTLP_LOGS_ENDPOINT OTEL_EXPORTER_OTLP_LOGS_PROTOCOL OTEL_EXPORTER_OTLP_LOGS_HEADERS OTEL_EXPORTER_OTLP_LOGS_TIMEOUT| ++
                  ~w|OTEL_EXPORTER_OTLP_METRICS_ENDPOINT OTEL_EXPORTER_OTLP_METRICS_PROTOCOL OTEL_EXPORTER_OTLP_METRICS_HEADERS OTEL_EXPORTER_OTLP_METRICS_TIMEOUT| ++
                  ~w|OTEL_METRICS_EXPORTER OTEL_LOGS_EXPORTER|

  setup do
    on_exit(fn ->
      for env <- @touched_envs do
        System.delete_env(env)
      end

      Application.get_all_env(:otel_metric_exporter)
      |> Keyword.keys()
      |> Enum.each(fn key ->
        Application.delete_env(:otel_metric_exporter, key)
      end)
    end)

    :ok
  end

  describe "defaults" do
    test "returns the default values" do
      assert OtelMetricExporter.OtelApi.Config.defaults() ==
               {:ok,
                %{
                  logs: %{
                    exporter: :otlp
                  },
                  metrics: %{
                    exporter: :otlp
                  },
                  otlp_compression: :gzip,
                  otlp_concurrent_requests: 10,
                  resource: %{},
                  otlp_headers: %{},
                  otlp_protocol: :http_protobuf,
                  otlp_timeout: 10000
                }}
    end

    test "can be overridden by env vars" do
      System.put_env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
      System.put_env("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:4318")
      System.put_env("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "key1=value1,key2=value2")
      System.put_env("OTEL_EXPORTER_OTLP_TIMEOUT", "10000")
      System.put_env("OTEL_METRICS_EXPORTER", "none")

      assert OtelMetricExporter.OtelApi.Config.defaults() ==
               {:ok,
                %{
                  logs: %{
                    exporter: :otlp,
                    otlp_endpoint: "http://localhost:4318"
                  },
                  metrics: %{
                    exporter: :none,
                    otlp_headers: %{"key1" => "value1", "key2" => "value2"}
                  },
                  otlp_compression: :gzip,
                  otlp_concurrent_requests: 10,
                  resource: %{},
                  otlp_headers: %{},
                  otlp_protocol: :http_protobuf,
                  otlp_timeout: 10000,
                  otlp_endpoint: "http://localhost:4317"
                }}
    end

    test "parses encoded separators, additional equals, and optional whitespace" do
      System.put_env(
        "OTEL_EXPORTER_OTLP_HEADERS",
        " alpha = one%2Ctwo , beta = literal=plus+%3D, gamma = lowercase%2cencoded%2b "
      )

      assert {:ok, %{otlp_headers: headers}} = OtelMetricExporter.OtelApi.Config.defaults()

      assert headers == %{
               "alpha" => "one,two",
               "beta" => "literal=plus+=",
               "gamma" => "lowercase,encoded+"
             }
    end

    test "parses signal-specific headers" do
      System.put_env("OTEL_EXPORTER_OTLP_LOGS_HEADERS", "logs=one%2Ctwo")

      assert {:ok, %{logs: %{otlp_headers: %{"logs" => "one,two"}}}} =
               OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "preserves allowed spaces in header values" do
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer placeholder-token")

      assert {:ok, %{otlp_headers: %{"authorization" => "Bearer placeholder-token"}}} =
               OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "rejects invalid application header values without exposing them" do
      secret = "application-header-secret"

      Application.put_env(:otel_metric_exporter, :otlp_headers, %{
        "authorization" => "bad\n#{secret}"
      })

      log =
        capture_log(fn ->
          assert {:error, error} = OtelMetricExporter.OtelApi.Config.defaults()
          assert error.key == :otlp_headers
          assert error.value == :redacted
          refute inspect(error) =~ secret
          refute Exception.message(error) =~ secret
        end)

      refute log =~ secret
    end

    test "redacts invalid nested application header values while preserving the path" do
      secret = "nested-application-header-secret"

      Application.put_env(:otel_metric_exporter, :logs, %{
        otlp_headers: %{"authorization" => "bad\n#{secret}"}
      })

      log =
        capture_log(fn ->
          assert {:error, error} = OtelMetricExporter.OtelApi.Config.defaults()
          assert error.key == :otlp_headers
          assert error.keys_path == [:logs]
          assert error.value == :redacted
          refute inspect(error) =~ secret
          refute Exception.message(error) =~ secret
        end)

      refute log =~ secret
    end

    test "discards malformed header members" do
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "valid=value,malformed")

      assert {:ok, %{otlp_headers: %{}}} = OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "discards headers with unsupported properties" do
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "key=value;property=unsupported")

      assert {:ok, %{otlp_headers: %{}}} = OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "discards headers with malformed percent encoding" do
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "key=value%2")

      assert {:ok, %{otlp_headers: %{}}} = OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "discards headers with decoded controls" do
      secret = "control-placeholder"
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "key=#{secret}%0A")

      log =
        capture_log(fn ->
          assert {:ok, %{otlp_headers: %{}}} = OtelMetricExporter.OtelApi.Config.defaults()
        end)

      assert log =~ "invalid_value"
      refute log =~ secret
    end

    test "discards headers with decoded non-ASCII values" do
      secret = "non-ascii-placeholder"
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "key=#{secret}%C3%A9")

      log =
        capture_log(fn ->
          assert {:ok, %{otlp_headers: %{}}} = OtelMetricExporter.OtelApi.Config.defaults()
        end)

      assert log =~ "invalid_value"
      refute log =~ secret
    end

    test "discards headers with invalid keys" do
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "invalid key=value")

      assert {:ok, %{otlp_headers: %{}}} = OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "redacts invalid header values from warnings" do
      secret = "placeholder-secret-token"
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer #{secret}%ZZ")

      log = capture_log(fn -> OtelMetricExporter.OtelApi.Config.defaults() end)

      assert log =~ "OTEL_EXPORTER_OTLP_HEADERS"
      assert log =~ "invalid_percent_encoding"
      refute log =~ "authorization"
      refute log =~ secret
    end

    test "treats empty signal-specific header env as unset" do
      System.put_env("OTEL_EXPORTER_OTLP_HEADERS", "generic=value")
      System.put_env("OTEL_EXPORTER_OTLP_LOGS_HEADERS", "")

      log =
        capture_log(fn ->
          assert {:ok, %{otlp_headers: %{"generic" => "value"}, logs: %{exporter: :otlp}}} =
                   OtelMetricExporter.OtelApi.Config.defaults()
        end)

      refute log =~ "Invalid OTEL_EXPORTER_OTLP_LOGS_HEADERS"
    end

    test "does not percent-decode headers from application config" do
      Application.put_env(:otel_metric_exporter, :otlp_headers, %{"key" => "one%2Ctwo+three"})

      assert {:ok, %{otlp_headers: %{"key" => "one%2Ctwo+three"}}} =
               OtelMetricExporter.OtelApi.Config.defaults()
    end

    test "can be set by application config that overrides env vars" do
      System.put_env("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:1010")
      System.put_env("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "key1=value1,key2=value2")

      Application.put_env(:otel_metric_exporter, :otlp_endpoint, "http://localhost:4317")

      Application.put_env(:otel_metric_exporter, :logs, otlp_endpoint: "http://localhost:4318")

      Application.put_env(:otel_metric_exporter, :metrics, %{exporter: :none})

      assert OtelMetricExporter.OtelApi.Config.defaults() ==
               {:ok,
                %{
                  logs: %{
                    exporter: :otlp,
                    otlp_endpoint: "http://localhost:4318"
                  },
                  metrics: %{
                    exporter: :none,
                    otlp_headers: %{"key1" => "value1", "key2" => "value2"}
                  },
                  otlp_compression: :gzip,
                  otlp_concurrent_requests: 10,
                  resource: %{},
                  otlp_headers: %{},
                  otlp_protocol: :http_protobuf,
                  otlp_timeout: 10000,
                  otlp_endpoint: "http://localhost:4317"
                }}
    end
  end

  describe "validate_for_scope/2" do
    test "accepts valid direct header maps" do
      assert {:ok,
              %OtelMetricExporter.OtelApi.Config{
                otlp_headers: %{"authorization" => "Bearer placeholder-token"}
              }, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{
                   otlp_endpoint: "http://localhost:4318",
                   otlp_headers: %{"authorization" => "Bearer placeholder-token"}
                 },
                 :logs
               )
    end

    test "rejects invalid direct header keys and values without exposing them" do
      secret = "direct-header-secret"

      log =
        capture_log(fn ->
          assert {:error, error} =
                   OtelMetricExporter.OtelApi.Config.validate_for_scope(
                     %{
                       otlp_endpoint: "http://localhost:4318",
                       otlp_headers: %{"invalid key" => "bad\n#{secret}"}
                     },
                     :logs
                   )

          assert error.key == :otlp_headers
          assert error.value == :redacted
          refute inspect(error) =~ secret
          refute Exception.message(error) =~ "invalid key"
          refute Exception.message(error) =~ secret
        end)

      refute log =~ secret
    end

    test "marks a direct endpoint as generic" do
      assert {:ok, %OtelMetricExporter.OtelApi.Config{otlp_endpoint_kind: :generic}, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{otlp_endpoint: "http://localhost:4318"},
                 :logs
               )
    end

    test "marks an environment signal endpoint as signal-specific" do
      System.put_env("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:4318/logs")

      assert {:ok, %OtelMetricExporter.OtelApi.Config{otlp_endpoint_kind: :signal}, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(%{}, :logs)
    end

    test "marks an application signal endpoint as signal-specific" do
      Application.put_env(:otel_metric_exporter, :metrics,
        otlp_endpoint: "http://localhost:4318/metrics"
      )

      assert {:ok, %OtelMetricExporter.OtelApi.Config{otlp_endpoint_kind: :signal}, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(%{}, :metrics)
    end

    test "marks a direct endpoint override as generic" do
      Application.put_env(:otel_metric_exporter, :logs,
        otlp_endpoint: "http://localhost:4318/logs"
      )

      assert {:ok, %OtelMetricExporter.OtelApi.Config{otlp_endpoint_kind: :generic}, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{otlp_endpoint: "http://localhost:4318"},
                 :logs
               )
    end

    test "continues ignoring direct nested signal options" do
      assert {:error, %{key: :otlp_endpoint}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{logs: %{otlp_endpoint: "http://localhost:4318/logs"}},
                 :logs
               )
    end

    test "returns error if otlp endpoint is not provided" do
      assert {:error, %{key: :otlp_endpoint}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{logs: %{exporter: :otlp}},
                 :logs
               )
    end

    test "merges the base config with the scope specific config" do
      System.put_env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
      System.put_env("OTEL_EXPORTER_OTLP_TIMEOUT", "5")

      Application.put_env(:otel_metric_exporter, :logs,
        otlp_headers: %{"key1" => "value1", "key2" => "value2"},
        otlp_timeout: 10
      )

      assert {:ok,
              %OtelMetricExporter.OtelApi.Config{
                exporter: :otlp,
                otlp_endpoint: "http://localhost:4318",
                otlp_headers: %{"key1" => "value1", "key2" => "value2"},
                otlp_timeout: 10
              }, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{logs: %{exporter: :otlp}},
                 :logs
               )
    end

    test "doesn't return an error if exporter is set to :none" do
      Application.put_env(:otel_metric_exporter, :logs, exporter: :none)

      assert {:ok, %OtelMetricExporter.OtelApi.Config{exporter: :none}, %{}} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{},
                 :logs
               )
    end

    @tag :otlp_validation_boundary
    test "rejects zero concurrent requests" do
      assert {:error, error} =
               OtelMetricExporter.OtelApi.Config.validate_for_scope(
                 %{otlp_endpoint: "http://localhost:4317", otlp_concurrent_requests: 0},
                 :logs
               )

      assert error.key == :otlp_concurrent_requests
      assert error.value == 0
    end
  end
end
