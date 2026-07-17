defmodule SymphonyElixir.Linear.RateLimiter do
  @moduledoc """
  Coordinates Linear rate-limit backoff across Symphony OS processes.

  Linear returns GraphQL rate-limit failures as HTTP 400 responses. The retry
  deadline is persisted under a token-scoped key so worker and reviewer
  services sharing the same Linear user stop together instead of hot-looping.
  """

  alias SymphonyElixir.Config

  @fallback_base_ms 30_000
  @fallback_max_ms 3_600_000
  @clock_skew_guard_ms 1_000

  @type details :: %{
          required(:retry_after_ms) => non_neg_integer(),
          required(:retry_at_unix_ms) => non_neg_integer(),
          required(:attempt) => pos_integer(),
          required(:source) => String.t(),
          optional(:persistence_error) => String.t()
        }

  @spec before_request(keyword()) :: :ok | {:error, {:linear_rate_limited, details()}}
  def before_request(opts \\ []) do
    now_ms = now_ms(opts)
    path = state_path(opts)

    case read_state(path) do
      %{"retry_at_unix_ms" => retry_at_ms} = state when is_integer(retry_at_ms) and retry_at_ms > now_ms ->
        {:error, {:linear_rate_limited, details_from_state(state, now_ms)}}

      %{} ->
        :ok

      nil ->
        :ok
    end
  end

  @spec observe_response(map(), keyword()) :: :ok | {:error, {:linear_rate_limited, details()}}
  def observe_response(response, opts \\ []) when is_map(response) do
    case rate_limit_metadata(response) do
      nil ->
        remove_expired_state(state_path(opts), now_ms(opts))
        :ok

      metadata ->
        persist_rate_limit(metadata, opts)
    end
  end

  @spec retry_after_ms(term()) :: non_neg_integer() | nil
  def retry_after_ms({:linear_rate_limited, %{retry_after_ms: retry_after_ms}})
      when is_integer(retry_after_ms),
      do: max(0, retry_after_ms)

  def retry_after_ms({:linear_rate_limited, %{"retry_after_ms" => retry_after_ms}})
      when is_integer(retry_after_ms),
      do: max(0, retry_after_ms)

  def retry_after_ms(_reason), do: nil

  @doc false
  @spec state_path_for_test(keyword()) :: Path.t()
  def state_path_for_test(opts), do: state_path(opts)

  defp persist_rate_limit(metadata, opts) do
    now_ms = now_ms(opts)
    path = state_path(opts)
    previous = read_state(path) || %{}
    attempt = max(Map.get(previous, "attempt", 0) + 1, 1)
    {delay_ms, source} = retry_delay(metadata, attempt, now_ms, opts)
    retry_at_ms = max(now_ms + delay_ms, Map.get(previous, "retry_at_unix_ms", 0))

    state = %{
      "attempt" => attempt,
      "retry_at_unix_ms" => retry_at_ms,
      "source" => source,
      "updated_at_unix_ms" => now_ms
    }

    details = details_from_state(state, now_ms)

    case write_state(path, state) do
      :ok ->
        {:error, {:linear_rate_limited, details}}

      {:error, reason} ->
        {:error, {:linear_rate_limited, Map.put(details, :persistence_error, inspect(reason))}}
    end
  end

  defp retry_delay(metadata, attempt, now_ms, opts) do
    cond do
      is_integer(metadata.reset_at_unix_ms) and metadata.reset_at_unix_ms > now_ms ->
        {metadata.reset_at_unix_ms - now_ms + @clock_skew_guard_ms, "reset_header"}

      is_integer(metadata.duration_ms) and metadata.duration_ms > 0 ->
        {metadata.duration_ms + @clock_skew_guard_ms, "response_duration"}

      true ->
        power = min(attempt - 1, 10)
        raw_delay = min(@fallback_base_ms * Integer.pow(2, power), @fallback_max_ms)
        {jitter(raw_delay, opts), "exponential_backoff"}
    end
  end

  defp jitter(delay_ms, opts) do
    jitter_fun = Keyword.get(opts, :jitter_fun, &:rand.uniform/1)
    spread = max(div(delay_ms, 5), 1)
    delay_ms - spread + jitter_fun.(spread * 2 + 1) - 1
  end

  defp rate_limit_metadata(response) do
    body = normalized_body(Map.get(response, :body) || Map.get(response, "body"))

    if rate_limited_body?(body) or embedded_status(response) == 429 do
      %{
        reset_at_unix_ms: reset_at_from_headers(response),
        duration_ms: duration_from_body(body)
      }
    end
  end

  defp normalized_body(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      _ -> body
    end
  end

  defp normalized_body(body), do: body

  defp rate_limited_body?(%{"errors" => errors}) when is_list(errors) do
    Enum.any?(errors, fn error ->
      get_in(error, ["extensions", "code"]) == "RATELIMITED" or
        get_in(error, ["extensions", "statusCode"]) == 429
    end)
  end

  defp rate_limited_body?(_body), do: false

  defp embedded_status(response), do: Map.get(response, :status) || Map.get(response, "status")

  defp duration_from_body(%{"errors" => errors}) when is_list(errors) do
    errors
    |> Enum.find_value(fn error ->
      get_in(error, ["extensions", "meta", "rateLimitResult", "duration"])
    end)
    |> positive_integer()
  end

  defp duration_from_body(_body), do: nil

  defp reset_at_from_headers(response) do
    headers = Map.get(response, :headers) || Map.get(response, "headers") || %{}

    [
      "x-ratelimit-endpoint-requests-reset",
      "x-ratelimit-requests-reset",
      "x-ratelimit-complexity-reset"
    ]
    |> Enum.find_value(&header_value(headers, &1))
    |> positive_integer()
  end

  defp header_value(headers, wanted) when is_map(headers) do
    headers
    |> Enum.find_value(fn {name, value} ->
      if String.downcase(to_string(name)) == wanted, do: first_header_value(value)
    end)
  end

  defp header_value(headers, wanted) when is_list(headers) do
    Enum.find_value(headers, fn
      {name, value} -> if String.downcase(to_string(name)) == wanted, do: first_header_value(value)
      _ -> nil
    end)
  end

  defp header_value(_headers, _wanted), do: nil
  defp first_header_value([value | _]), do: value
  defp first_header_value(value), do: value

  defp positive_integer(value) when is_integer(value) and value > 0, do: value

  defp positive_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> parsed
      _ -> nil
    end
  end

  defp positive_integer(_value), do: nil

  defp details_from_state(state, now_ms) do
    retry_at_ms = Map.fetch!(state, "retry_at_unix_ms")

    %{
      retry_after_ms: max(0, retry_at_ms - now_ms),
      retry_at_unix_ms: retry_at_ms,
      attempt: Map.get(state, "attempt", 1),
      source: Map.get(state, "source", "persisted_gate")
    }
  end

  defp state_path(opts) do
    Keyword.get_lazy(opts, :path, fn ->
      tracker = Config.settings!().tracker
      fingerprint = :crypto.hash(:sha256, "#{tracker.endpoint}\0#{tracker.api_key}") |> Base.encode16(case: :lower)
      Path.join([state_root(), "linear_rate_limits", fingerprint <> ".json"])
    end)
  end

  defp state_root do
    System.get_env("SYMPHONY_STATE_DIR") ||
      Path.join([System.user_home!(), ".local", "state", "symphony"])
  end

  defp now_ms(opts), do: Keyword.get(opts, :now_ms, System.system_time(:millisecond))

  defp read_state(path) do
    with {:ok, raw} <- File.read(path),
         {:ok, %{} = state} <- Jason.decode(raw) do
      state
    else
      _ -> nil
    end
  end

  defp write_state(path, state) do
    temporary = path <> ".tmp-#{System.unique_integer([:positive])}"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary, Jason.encode!(state)),
         :ok <- File.rename(temporary, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary)
        {:error, reason}
    end
  end

  defp remove_state(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp remove_expired_state(path, now_ms) do
    case read_state(path) do
      %{"retry_at_unix_ms" => retry_at_ms} when is_integer(retry_at_ms) and retry_at_ms > now_ms -> :ok
      _ -> remove_state(path)
    end
  end
end
