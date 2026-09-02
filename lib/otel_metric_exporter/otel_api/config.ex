defmodule OtelMetricExporter.OtelApi.Config do
  require Logger

  defstruct [
    :otlp_endpoint,
    :otlp_endpoint_kind,
    :otlp_protocol,
    :otlp_headers,
    :otlp_timeout,
    :exporter,
    :resource,
    :otlp_compression,
    :otlp_concurrent_requests
  ]

  @type protocol :: :http_protobuf
  @type compression :: :gzip | nil
  @type endpoint_kind :: :generic | :signal
  @typep header_parse_error ::
           :malformed_member
           | :unsupported_properties
           | :invalid_key
           | :invalid_percent_encoding
           | :invalid_value

  endpoint_opt = [
    otlp_endpoint: [
      type: :string,
      doc: "Endpoint to send data to."
    ]
  ]

  otlp_options =
    [
      otlp_protocol: [
        type: {:in, [:http_protobuf]},
        type_spec: quote(do: protocol()),
        default: :http_protobuf,
        doc: "Protocol to use for OTLP export. Currently only `:http_protobuf` is supported."
      ],
      otlp_headers: [
        type: {:custom, __MODULE__, :validate_otlp_headers, []},
        type_spec: quote(do: %{String.t() => String.t()}),
        default: %{},
        doc: "Headers to send with OTLP requests."
      ],
      otlp_timeout: [
        type: :pos_integer,
        default: 10_000,
        doc: "Timeout for OTLP requests."
      ]
    ]

  exporter_opt = [exporter: [type: {:in, [:otlp, :none]}, default: :otlp]]

  specific_option =
    for scope <- [:logs, :metrics] do
      {scope,
       [
         type: {:or, [:map, :keyword_list]},
         required: false,
         doc: "Overrides for #{scope}.",
         keys:
           Enum.map(endpoint_opt ++ otlp_options, fn {key, opts} ->
             {key, opts |> Keyword.delete(:default)}
           end) ++ exporter_opt
       ]}
    end

  top_level_opts = [
    otlp_compression: [
      type: {:in, [:gzip, nil]},
      default: :gzip,
      type_spec: quote(do: compression()),
      doc: "Compression to use for OTLP requests. Allowed values are `:gzip` and `nil`."
    ],
    otlp_concurrent_requests: [
      type: :pos_integer,
      default: 10,
      doc: "Number of concurrent requests to send to the OTLP endpoint. Must be positive."
    ],
    resource: [
      type: {:map, {:or, [:atom, :string]}, :any},
      default: %{},
      doc: "Resource attributes to send with collected data."
    ]
  ]

  @public_options endpoint_opt ++
                    otlp_options ++
                    specific_option ++ top_level_opts

  @options_schema NimbleOptions.new!(@public_options)

  @single_scope_schema NimbleOptions.new!(
                         [
                           otlp_endpoint: [
                             type: :string,
                             required: true,
                             doc: "Endpoint to send data to."
                           ]
                         ] ++ otlp_options ++ exporter_opt ++ top_level_opts
                       )

  def public_options(), do: @public_options
  def options_schema(), do: @options_schema

  @doc false
  @spec validate(keyword() | map(), NimbleOptions.t() | NimbleOptions.schema()) ::
          {:ok, keyword() | map()} | {:error, NimbleOptions.ValidationError.t()}
  def validate(options, schema) do
    case NimbleOptions.validate(options, schema) do
      {:error, %NimbleOptions.ValidationError{key: :otlp_headers} = error} ->
        {:error, %{error | value: :redacted}}

      result ->
        result
    end
  end

  @doc false
  @spec validate_otlp_headers(term()) ::
          {:ok, %{String.t() => String.t()}} | {:error, String.t()}
  def validate_otlp_headers(headers) when is_map(headers) do
    if Enum.all?(headers, fn {key, value} ->
         is_binary(key) and is_binary(value) and valid_http_token?(key) and
           valid_header_value_bytes?(value)
       end) do
      {:ok, headers}
    else
      {:error, "invalid HTTP header key or value"}
    end
  end

  def validate_otlp_headers(_headers), do: {:error, "expected a map of HTTP headers"}

  defp defaults_from_env do
    base =
      %{}
      |> put_from_env("OTEL_EXPORTER_OTLP_ENDPOINT", :otlp_endpoint)
      |> put_from_env("OTEL_EXPORTER_OTLP_PROTOCOL", :otlp_protocol, &cast_otlp_protocol/1)
      |> put_from_env("OTEL_EXPORTER_OTLP_HEADERS", :otlp_headers, &cast_otlp_headers/1)
      |> put_from_env("OTEL_EXPORTER_OTLP_TIMEOUT", :otlp_timeout, &cast_otlp_timeout/1)

    for scope <- [:logs, :metrics], env_scope = String.upcase(to_string(scope)), reduce: base do
      acc ->
        acc
        |> Map.put(scope, %{})
        |> put_from_env("OTEL_EXPORTER_OTLP_#{env_scope}_ENDPOINT", [scope, :otlp_endpoint])
        |> put_from_env(
          "OTEL_EXPORTER_OTLP_#{env_scope}_PROTOCOL",
          [scope, :otlp_protocol],
          &cast_otlp_protocol/1
        )
        |> put_from_env(
          "OTEL_EXPORTER_OTLP_#{env_scope}_HEADERS",
          [scope, :otlp_headers],
          &cast_otlp_headers/1
        )
        |> put_from_env(
          "OTEL_EXPORTER_OTLP_#{env_scope}_TIMEOUT",
          [scope, :otlp_timeout],
          &cast_otlp_timeout/1
        )
        |> put_from_env("OTEL_#{env_scope}_EXPORTER", [scope, :exporter], &cast_otlp_exporter/1)
    end
    |> put_from_env("OTEL_RESOURCE_ATTRIBUTES", :resource, &cast_resource_attributes/1)
    |> Map.put_new(:resource, %{})
    |> put_from_env("OTEL_SERVICE_NAME", [:resource, "service.name"])
  end

  defp put_from_env(acc, env_var, key, cast_fun \\ fn x -> {:ok, x} end) do
    case System.fetch_env(env_var) do
      :error ->
        acc

      {:ok, ""} ->
        acc

      {:ok, value} ->
        case cast_fun.(value) do
          {:ok, casted} ->
            put_in(acc, List.wrap(key), casted)

          {:error, message} ->
            Logger.warning("Invalid #{env_var} value, ignoring: #{message}")
            acc
        end
    end
  end

  defp cast_otlp_protocol("http/json"), do: {:ok, :http_json}
  defp cast_otlp_protocol("http/protobuf"), do: {:ok, :http_protobuf}
  defp cast_otlp_protocol("grpc"), do: {:ok, :grpc}
  defp cast_otlp_protocol(other), do: {:error, "invalid OTLP protocol: #{inspect(other)}"}

  @spec cast_otlp_headers(String.t()) ::
          {:ok, %{String.t() => String.t()}} | {:error, header_parse_error()}
  defp cast_otlp_headers(value) do
    value
    |> String.split(",", trim: false)
    |> Enum.reduce_while(%{}, fn member, headers ->
      case parse_header_member(member) do
        {:ok, {key, value}} -> {:cont, Map.put(headers, key, value)}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      headers -> {:ok, headers}
    end
  end

  @spec parse_header_member(String.t()) ::
          {:ok, {String.t(), String.t()}} | {:error, header_parse_error()}
  defp parse_header_member(member) do
    member = trim_optional_whitespace(member)

    cond do
      member == "" -> {:error, :malformed_member}
      :binary.match(member, ";") != :nomatch -> {:error, :unsupported_properties}
      true -> split_header_member(member)
    end
  end

  @spec split_header_member(String.t()) ::
          {:ok, {String.t(), String.t()}} | {:error, header_parse_error()}
  defp split_header_member(member) do
    case :binary.match(member, "=") do
      :nomatch ->
        {:error, :malformed_member}

      {separator, 1} ->
        key = member |> binary_part(0, separator) |> trim_optional_whitespace()

        value =
          member
          |> binary_part(separator + 1, byte_size(member) - separator - 1)
          |> trim_optional_whitespace()

        with true <- valid_http_token?(key),
             {:ok, value} <- decode_header_value(value) do
          {:ok, {key, value}}
        else
          false -> {:error, :invalid_key}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec decode_header_value(String.t()) :: {:ok, String.t()} | {:error, header_parse_error()}
  defp decode_header_value(value) do
    with {:ok, decoded} <- percent_decode(value, []),
         true <- valid_header_value_bytes?(decoded) do
      {:ok, decoded}
    else
      {:error, reason} -> {:error, reason}
      false -> {:error, :invalid_value}
    end
  end

  @spec percent_decode(binary(), [binary()]) ::
          {:ok, binary()} | {:error, :invalid_percent_encoding}
  defp percent_decode(<<>>, acc), do: {:ok, IO.iodata_to_binary(Enum.reverse(acc))}

  defp percent_decode(<<"%", high, low, rest::binary>>, acc) do
    with {:ok, high} <- hex_digit(high),
         {:ok, low} <- hex_digit(low) do
      percent_decode(rest, [<<high * 16 + low>> | acc])
    else
      :error -> {:error, :invalid_percent_encoding}
    end
  end

  defp percent_decode(<<"%", _rest::binary>>, _acc), do: {:error, :invalid_percent_encoding}
  defp percent_decode(<<byte, rest::binary>>, acc), do: percent_decode(rest, [<<byte>> | acc])

  @spec hex_digit(byte()) :: {:ok, 0..15} | :error
  defp hex_digit(char) when char in ?0..?9, do: {:ok, char - ?0}
  defp hex_digit(char) when char in ?A..?F, do: {:ok, char - ?A + 10}
  defp hex_digit(char) when char in ?a..?f, do: {:ok, char - ?a + 10}
  defp hex_digit(_char), do: :error

  @spec valid_header_value_bytes?(binary()) :: boolean()
  defp valid_header_value_bytes?(<<>>), do: true

  defp valid_header_value_bytes?(<<byte, rest::binary>>)
       when byte == 9 or byte in 0x20..0x7E,
       do: valid_header_value_bytes?(rest)

  defp valid_header_value_bytes?(_value), do: false

  @spec valid_http_token?(binary()) :: boolean()
  defp valid_http_token?(<<first, rest::binary>>) do
    http_token_char?(first) and valid_http_token_bytes?(rest)
  end

  defp valid_http_token?(_), do: false

  @spec valid_http_token_bytes?(binary()) :: boolean()
  defp valid_http_token_bytes?(<<>>), do: true

  defp valid_http_token_bytes?(<<char, rest::binary>>),
    do: http_token_char?(char) and valid_http_token_bytes?(rest)

  @spec http_token_char?(byte()) :: boolean()
  defp http_token_char?(char)
       when char in ?A..?Z or char in ?a..?z or char in ?0..?9 or
              char in [?!, ?#, ?$, ?%, ?&, ?', ?*, ?+, ?-, ?., ?^, ?_, ?`, ?|, ?~],
       do: true

  defp http_token_char?(_char), do: false

  @spec trim_optional_whitespace(binary()) :: binary()
  defp trim_optional_whitespace(<<" ", rest::binary>>), do: trim_optional_whitespace(rest)
  defp trim_optional_whitespace(<<"\t", rest::binary>>), do: trim_optional_whitespace(rest)

  defp trim_optional_whitespace(value), do: trim_optional_whitespace_right(value)

  @spec trim_optional_whitespace_right(binary()) :: binary()
  defp trim_optional_whitespace_right(<<>>), do: <<>>

  defp trim_optional_whitespace_right(value) do
    case :binary.last(value) do
      ?\s -> trim_optional_whitespace_right(binary_part(value, 0, byte_size(value) - 1))
      ?\t -> trim_optional_whitespace_right(binary_part(value, 0, byte_size(value) - 1))
      _ -> value
    end
  end

  defp cast_otlp_timeout(value) do
    case Integer.parse(value) do
      {timeout, ""} when timeout > 0 -> {:ok, timeout}
      _ -> {:error, "invalid OTLP timeout: #{inspect(value)}"}
    end
  end

  defp cast_resource_attributes(value) do
    value
    |> String.split(",")
    |> Enum.map(&String.split(&1, "="))
    |> Map.new(fn [key, value] -> {key, value} end)
    |> then(&{:ok, &1})
  rescue
    FunctionClauseError ->
      {:error, "invalid resource attributes: #{inspect(value)}"}
  end

  defp cast_otlp_exporter("otlp"), do: {:ok, :otlp}
  defp cast_otlp_exporter("none"), do: {:ok, :none}
  defp cast_otlp_exporter(_), do: {:error, "unsupported otlp exporter configuration"}

  def defaults do
    from_env =
      defaults_from_env()
      |> Map.take(Keyword.keys(@public_options))
      |> validate(options_schema())
      |> case do
        {:ok, validated} -> validated
        {:error, _} -> %{}
      end

    from_app =
      Application.get_all_env(:otel_metric_exporter)
      |> Map.new()
      |> Map.take(Keyword.keys(@public_options))
      |> Map.update(:resource, %{}, &normalize_resources/1)

    Map.merge(from_env, from_app, fn
      k, v1, v2 when k in [:logs, :metrics, :resource] -> Map.merge(Map.new(v1), Map.new(v2))
      _, _, v2 -> v2
    end)
    |> validate(options_schema())
  end

  @spec validate_for_scope(map(), :logs | :metrics) ::
          {:ok, %__MODULE__{}, map()} | {:error, NimbleOptions.ValidationError.t()}
  def validate_for_scope(config, scope) when scope in [:logs, :metrics] do
    {provided, rest} = Map.split(config, Keyword.keys(@public_options) -- [:logs, :metrics])

    with {:ok, defaults} <- defaults() do
      endpoint_kind = endpoint_kind(provided, defaults, scope)

      with_overrides =
        defaults
        |> get_for_scope(scope)
        |> Map.merge(
          Map.update(provided, :resource, %{}, &normalize_resources/1),
          &if(&1 == :resource, do: Map.merge(&2, &3), else: &3)
        )

      case Map.get(with_overrides, :exporter) do
        x when x in [:otlp, nil] ->
          with {:ok, validated} <- validate(with_overrides, @single_scope_schema) do
            {:ok, struct!(__MODULE__, Map.put(validated, :otlp_endpoint_kind, endpoint_kind)),
             rest}
          end

        :none ->
          {:ok, %__MODULE__{exporter: :none, otlp_endpoint_kind: endpoint_kind}, rest}
      end
    end
  end

  @doc """
  Validates an already-resolved signal configuration without reading application
  or environment defaults.

  The input must include its effective `:exporter` and `:otlp_endpoint_kind`.
  This boundary is for retaining installed runtime state during partial updates;
  callers that are resolving public configuration must use `validate_for_scope/2`.
  """
  @spec validate_effective_for_scope(map(), :logs | :metrics) ::
          {:ok, %__MODULE__{}, map()} | {:error, term()}
  def validate_effective_for_scope(config, scope) when scope in [:logs, :metrics] do
    effective_keys = Keyword.keys(@public_options) ++ [:otlp_endpoint_kind, :exporter]
    {effective, rest} = Map.split(Map.new(config), effective_keys)
    {endpoint_kind, effective} = Map.pop(effective, :otlp_endpoint_kind)
    effective = Map.update(effective, :resource, %{}, &normalize_resources/1)

    case {Map.get(effective, :exporter), endpoint_kind} do
      {:none, endpoint_kind} when endpoint_kind in [:generic, :signal] ->
        {:ok, %__MODULE__{exporter: :none, otlp_endpoint_kind: endpoint_kind}, rest}

      {:otlp, endpoint_kind} when endpoint_kind in [:generic, :signal] ->
        with {:ok, validated} <- validate(effective, @single_scope_schema) do
          {:ok, struct!(__MODULE__, Map.put(validated, :otlp_endpoint_kind, endpoint_kind)), rest}
        end

      {exporter, _endpoint_kind} ->
        {:error, {:invalid_effective_exporter, exporter}}
    end
  end

  @spec endpoint_kind(map(), map(), :logs | :metrics) :: endpoint_kind()
  defp endpoint_kind(provided, defaults, scope) do
    cond do
      Map.has_key?(provided, :otlp_endpoint) -> :generic
      Map.has_key?(Map.get(defaults, scope, %{}), :otlp_endpoint) -> :signal
      true -> :generic
    end
  end

  defp get_for_scope(config, scope),
    do: Map.merge(Map.drop(config, [:logs, :metrics]), config[scope] || %{})

  defp normalize_resources(resource_map), do: Map.new(do_normalize_resources(resource_map))

  defp do_normalize_resources(map, prefix \\ "") do
    Enum.flat_map(map, fn {key, value} ->
      if is_map(value) do
        do_normalize_resources(value, prefix <> to_string(key) <> ".")
      else
        [{prefix <> to_string(key), value}]
      end
    end)
  end
end
