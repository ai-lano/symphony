defmodule SymphonyElixir.Linear.RateLimiterTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Linear.{Client, RateLimiter}

  setup do
    path = Path.join(System.tmp_dir!(), "linear-rate-limit-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(path) end)
    %{path: path}
  end

  test "classifies GraphQL HTTP 400 RATELIMITED and honors the reset header", %{path: path} do
    response = %{
      status: 400,
      headers: %{"x-ratelimit-requests-reset" => ["62000"]},
      body: %{
        "errors" => [
          %{
            "extensions" => %{
              "code" => "RATELIMITED",
              "statusCode" => 429,
              "meta" => %{"rateLimitResult" => %{"duration" => 3_600_000}}
            }
          }
        ]
      }
    }

    assert {:error, {:linear_rate_limited, %{retry_after_ms: 3_000, retry_at_unix_ms: 63_000, source: "reset_header"}}} =
             RateLimiter.observe_response(response, path: path, now_ms: 60_000)

    assert {:error, {:linear_rate_limited, %{retry_after_ms: 2_000}}} =
             RateLimiter.before_request(path: path, now_ms: 61_000)

    assert :ok = RateLimiter.before_request(path: path, now_ms: 63_000)
    refute File.exists?(path)
  end

  test "uses response duration when reset headers are missing", %{path: path} do
    response = %{
      status: 400,
      body: %{
        "errors" => [
          %{
            "extensions" => %{
              "code" => "RATELIMITED",
              "meta" => %{"rateLimitResult" => %{"duration" => 120_000}}
            }
          }
        ]
      }
    }

    assert {:error, {:linear_rate_limited, details}} =
             RateLimiter.observe_response(response, path: path, now_ms: 10_000)

    assert details.retry_after_ms == 121_000
    assert details.source == "response_duration"
  end

  test "falls back to jittered exponential backoff for plain HTTP 429", %{path: path} do
    response = %{status: 429, body: "too many requests"}

    assert {:error, {:linear_rate_limited, details}} =
             RateLimiter.observe_response(response,
               path: path,
               now_ms: 10_000,
               jitter_fun: fn _range -> 1 end
             )

    assert details.retry_after_ms == 24_000
    assert details.source == "exponential_backoff"
  end

  test "a successful response clears an expired persisted gate", %{path: path} do
    File.write!(
      path,
      Jason.encode!(%{
        "attempt" => 1,
        "retry_at_unix_ms" => 1,
        "source" => "test"
      })
    )

    assert :ok = RateLimiter.observe_response(%{status: 200, body: %{"data" => %{}}}, path: path)
    refute File.exists?(path)
  end

  test "the Linear client suppresses a second network request while the shared gate is active" do
    state_root =
      Path.join(System.tmp_dir!(), "linear-client-rate-limit-#{System.unique_integer([:positive])}")

    previous_state_root = System.get_env("SYMPHONY_STATE_DIR")
    System.put_env("SYMPHONY_STATE_DIR", state_root)

    on_exit(fn ->
      if previous_state_root,
        do: System.put_env("SYMPHONY_STATE_DIR", previous_state_root),
        else: System.delete_env("SYMPHONY_STATE_DIR")

      File.rm_rf(state_root)
    end)

    response = %{
      status: 400,
      headers: %{},
      body: %{
        "errors" => [
          %{
            "extensions" => %{
              "code" => "RATELIMITED",
              "meta" => %{"rateLimitResult" => %{"duration" => 60_000}}
            }
          }
        ]
      }
    }

    assert {:error, {:linear_rate_limited, _details}} =
             Client.graphql("query Viewer { viewer { id } }", %{}, request_fun: fn _payload, _headers -> {:ok, response} end)

    assert {:error, {:linear_rate_limited, _details}} =
             Client.graphql("query Viewer { viewer { id } }", %{}, request_fun: fn _payload, _headers -> flunk("the gated request must not reach Linear") end)
  end

  test "still classifies a rate limit when the shared state file cannot be written" do
    response = %{status: 429, body: "too many requests"}

    assert {:error, {:linear_rate_limited, %{persistence_error: persistence_error}}} =
             RateLimiter.observe_response(response,
               path: "/dev/null/rate-limit.json",
               now_ms: 10_000,
               jitter_fun: fn _range -> 1 end
             )

    assert is_binary(persistence_error)
  end
end
