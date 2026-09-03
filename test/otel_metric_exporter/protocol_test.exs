defmodule OtelMetricExporter.ProtocolTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.Protocol
  alias OtelMetricExporter.OtlpUtils

  @config %{
    metadata: [:request_id, :stack_id, :span_id],
    metadata_map: %{request_id: "http.request.id"}
  }

  describe "build_log_service_request" do
    test "correctly encodes report messages" do
      events = [
        Protocol.prepare_log_event(
          %{
            level: :info,
            msg: {:report, request_id: "req-aaaa", stack_id: "stack-aaaa"},
            meta: %{time: System.system_time(:millisecond)}
          },
          @config
        )
      ]

      msg =
        Protocol.build_log_service_request(events)
        |> Protobuf.encode_to_iodata()
        |> IO.iodata_to_binary()

      assert is_binary(msg)
    end
  end

  describe "OTLP attribute values" do
    test "encodes booleans as boolean values and atoms as strings" do
      assert {:bool_value, true} = OtlpUtils.to_kv_value(true)
      assert {:bool_value, false} = OtlpUtils.to_kv_value(false)
      assert {:string_value, "normal"} = OtlpUtils.to_kv_value(:normal)
    end

    test "encodes nested structs and arbitrary map keys" do
      uri = URI.parse("https://example.com/packages")

      values =
        OtlpUtils.build_kv(%{%{} => "map key", nested: uri})
        |> Map.new(fn key_value -> {key_value.key, key_value.value} end)

      assert %{value: {:string_value, encoded_uri}} = values["nested"]
      assert encoded_uri == inspect(uri)
      assert %{value: {:string_value, "map key"}} = values["%{}"]
    end
  end
end
