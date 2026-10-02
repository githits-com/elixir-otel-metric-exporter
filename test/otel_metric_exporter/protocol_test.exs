defmodule OtelMetricExporter.ProtocolTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.Protocol
  alias OtelMetricExporter.OtlpUtils
  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{AnyValue, ArrayValue, KeyValueList}

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

  describe "OTLP list values" do
    test "encodes mixed proper lists as arrays without losing tuple elements" do
      pair = array([{:string_value, "a"}, {:int_value, 1}])

      for {value, expected} <- [
            {[{:a, 1}, :b], array([pair, {:string_value, "b"}])},
            {[{:a, 1}, "b"], array([pair, {:string_value, "b"}])},
            {[{:a, 1}, 2], array([pair, {:int_value, 2}])},
            {[[{:a, 1}, :b]], array([array([pair, {:string_value, "b"}])])}
          ] do
        assert OtlpUtils.to_kv_value(value) == expected
      end
    end

    test "mixed atom and string keys are tuple arrays, not accidental kvlists" do
      string_pair = array([{:string_value, "b"}, {:int_value, 2}])

      assert OtlpUtils.to_kv_value([{:a, 1}, {"b", 2}]) ==
               array([array([{:string_value, "a"}, {:int_value, 1}]), string_pair])

      assert OtlpUtils.to_kv_value([{"b", 2}]) == array([string_pair])

      assert OtlpUtils.to_kv_value([{3, :value}]) ==
               array([array([{:int_value, 3}, {:string_value, "value"}])])
    end

    test "keyword lists preserve order and duplicate keys, including nested values" do
      assert {:kvlist_value, %KeyValueList{values: values}} =
               OtlpUtils.to_kv_value(a: 1, a: 2, nested: [child: 3])

      assert Enum.map(values, &{&1.key, &1.value.value}) == [
               {"a", {:int_value, 1}},
               {"a", {:int_value, 2}},
               {"nested",
                {:kvlist_value,
                 %KeyValueList{
                   values: [
                     %OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue{
                       key: "child",
                       value: %AnyValue{value: {:int_value, 3}}
                     }
                   ]
                 }}}
             ]
    end

    test "empty lists, ordinary arrays and tuples retain array representation" do
      assert OtlpUtils.to_kv_value([]) == array([])
      assert OtlpUtils.to_kv_value([1, "two"]) == array([{:int_value, 1}, {:string_value, "two"}])
      assert OtlpUtils.to_kv_value({:a, 1}) == array([{:string_value, "a"}, {:int_value, 1}])
    end

    test "improper lists use inspected strings at each nested value boundary" do
      for improper <- [[1 | 2], [{:a, 1} | :tail], [1, 2 | :tail]] do
        expected = {:string_value, inspect(improper)}
        assert OtlpUtils.to_kv_value(improper) == expected
        assert OtlpUtils.to_kv_value([improper]) == array([expected])

        assert OtlpUtils.to_kv_value({:value, improper}) ==
                 array([{:string_value, "value"}, expected])

        assert {:kvlist_value, %KeyValueList{values: [value]}} =
                 OtlpUtils.to_kv_value(payload: improper)

        assert value.key == "payload"
        assert value.value.value == expected
      end
    end

    test "report map payloads survive a protobuf round trip" do
      pair = array([{:string_value, "a"}, {:int_value, 1}])

      for {payload, expected} <- [
            {[{:a, 1}, :b], array([pair, {:string_value, "b"}])},
            {[[1 | 2]], array([{:string_value, inspect([1 | 2])}])},
            {[nested: [1 | 2]],
             {:kvlist_value,
              %KeyValueList{
                values: [
                  %OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue{
                    key: "nested",
                    value: %AnyValue{value: {:string_value, inspect([1 | 2])}}
                  }
                ]
              }}}
          ] do
        prepared =
          Protocol.prepare_log_event(
            %{level: :info, msg: {:report, %{payload: payload}}, meta: %{time: 1}},
            @config
          )

        request =
          [prepared]
          |> Protocol.build_log_service_request()
          |> Protobuf.encode()
          |> Protobuf.decode(ExportLogsServiceRequest)

        assert %{resource_logs: [%{scope_logs: [%{log_records: [record]}]}]} = request
        assert {:kvlist_value, %KeyValueList{values: [value]}} = record.body.value
        assert value.key == "payload"
        assert value.value.value == expected
      end
    end
  end

  defp array(values),
    do: {:array_value, %ArrayValue{values: Enum.map(values, &%AnyValue{value: &1})}}
end
