defmodule SymphonyElixir.Codex.DynamicTool do
  @moduledoc """
  Executes client-side tool calls requested by Codex app-server turns.
  """

  alias SymphonyElixir.Linear.{Client, PendingHandoff}
  alias SymphonyElixir.Workspace

  @linear_graphql_tool "linear_graphql"
  @linear_issue_handoff_tool "linear_issue_handoff"
  @linear_graphql_description """
  Execute a raw GraphQL query or mutation against Linear using Symphony's configured auth.
  """
  @linear_graphql_input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["query"],
    "properties" => %{
      "query" => %{
        "type" => "string",
        "description" => "GraphQL query or mutation document to execute against Linear."
      },
      "variables" => %{
        "type" => ["object", "null"],
        "description" => "Optional GraphQL variables object.",
        "additionalProperties" => true
      }
    }
  }
  @linear_issue_handoff_description """
  Durably hand an issue to another Linear workflow state. The transition is persisted before acknowledgement and retried without redispatching the agent.
  """
  @linear_issue_handoff_input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["issue_id", "state_name"],
    "properties" => %{
      "issue_id" => %{
        "type" => "string",
        "description" => "Linear issue UUID, not the human-readable identifier."
      },
      "state_name" => %{
        "type" => "string",
        "description" => "Exact target workflow state name, such as In Review or Todo."
      }
    }
  }

  @spec execute(String.t() | nil, term(), keyword()) :: map()
  def execute(tool, arguments, opts \\ []) do
    case tool do
      @linear_graphql_tool ->
        execute_linear_graphql(arguments, opts)

      @linear_issue_handoff_tool ->
        execute_linear_issue_handoff(arguments, opts)

      other ->
        failure_response(%{
          "error" => %{
            "message" => "Unsupported dynamic tool: #{inspect(other)}.",
            "supportedTools" => supported_tool_names()
          }
        })
    end
  end

  @spec tool_specs() :: [map()]
  def tool_specs do
    [
      %{
        "name" => @linear_graphql_tool,
        "description" => @linear_graphql_description,
        "inputSchema" => @linear_graphql_input_schema
      },
      %{
        "name" => @linear_issue_handoff_tool,
        "description" => @linear_issue_handoff_description,
        "inputSchema" => @linear_issue_handoff_input_schema
      }
    ]
  end

  defp execute_linear_issue_handoff(arguments, opts) when is_map(arguments) do
    handoff_fun = Keyword.get(opts, :handoff_fun, &PendingHandoff.enqueue/2)

    handoff_guard_fun =
      Keyword.get(opts, :handoff_guard_fun, fn issue_id, state_name ->
        run_handoff_guard(issue_id, state_name, opts)
      end)

    issue_id = Map.get(arguments, "issue_id") || Map.get(arguments, :issue_id)
    state_name = Map.get(arguments, "state_name") || Map.get(arguments, :state_name)

    with true <- is_binary(issue_id) and String.trim(issue_id) != "",
         true <- is_binary(state_name) and String.trim(state_name) != "",
         :ok <- handoff_guard_fun.(issue_id, state_name),
         {:ok, :queued} <- handoff_fun.(issue_id, state_name) do
      dynamic_tool_response(
        true,
        encode_payload(%{
          "handoff" => %{
            "issue_id" => issue_id,
            "state_name" => state_name,
            "status" => "queued",
            "durable" => true
          }
        })
      )
    else
      false ->
        failure_response(%{
          "error" => %{
            "message" => "`linear_issue_handoff` requires non-empty `issue_id` and `state_name` strings."
          }
        })

      {:error, {:handoff_guard_failed, reason}} ->
        failure_response(%{
          "error" => %{
            "message" => "The pre-handoff gate failed; the issue state was not queued.",
            "reason" => inspect(reason)
          }
        })

      {:error, reason} ->
        failure_response(%{
          "error" => %{
            "message" => "Failed to persist the Linear issue handoff.",
            "reason" => inspect(reason)
          }
        })
    end
  end

  defp execute_linear_issue_handoff(_arguments, _opts) do
    failure_response(%{
      "error" => %{
        "message" => "`linear_issue_handoff` expects an object with `issue_id` and `state_name`."
      }
    })
  end

  defp run_handoff_guard(_issue_id, state_name, opts) do
    case Keyword.fetch(opts, :workspace) do
      {:ok, workspace} ->
        issue = Keyword.get(opts, :issue)
        worker_host = Keyword.get(opts, :worker_host)

        case Workspace.run_before_handoff_hook(workspace, issue, state_name, worker_host) do
          :ok -> :ok
          {:error, reason} -> {:error, {:handoff_guard_failed, reason}}
        end

      :error ->
        :ok
    end
  end

  defp execute_linear_graphql(arguments, opts) do
    linear_client = Keyword.get(opts, :linear_client, &Client.graphql/3)

    with {:ok, query, variables} <- normalize_linear_graphql_arguments(arguments),
         {:ok, response} <- linear_client.(query, variables, []) do
      graphql_response(response)
    else
      {:error, reason} ->
        failure_response(tool_error_payload(reason))
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_binary(arguments) do
    case String.trim(arguments) do
      "" -> {:error, :missing_query}
      query -> {:ok, query, %{}}
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_map(arguments) do
    case normalize_query(arguments) do
      {:ok, query} ->
        case normalize_variables(arguments) do
          {:ok, variables} ->
            {:ok, query, variables}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_linear_graphql_arguments(_arguments), do: {:error, :invalid_arguments}

  defp normalize_query(arguments) do
    case Map.get(arguments, "query") || Map.get(arguments, :query) do
      query when is_binary(query) ->
        case String.trim(query) do
          "" -> {:error, :missing_query}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, :missing_query}
    end
  end

  defp normalize_variables(arguments) do
    case Map.get(arguments, "variables") || Map.get(arguments, :variables) || %{} do
      variables when is_map(variables) -> {:ok, variables}
      _ -> {:error, :invalid_variables}
    end
  end

  defp graphql_response(response) do
    success =
      case response do
        %{"errors" => errors} when is_list(errors) and errors != [] -> false
        %{errors: errors} when is_list(errors) and errors != [] -> false
        _ -> true
      end

    dynamic_tool_response(success, encode_payload(response))
  end

  defp failure_response(payload) do
    dynamic_tool_response(false, encode_payload(payload))
  end

  defp dynamic_tool_response(success, output) when is_boolean(success) and is_binary(output) do
    %{
      "success" => success,
      "output" => output,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => output
        }
      ]
    }
  end

  defp encode_payload(payload) when is_map(payload) or is_list(payload) do
    Jason.encode!(payload, pretty: true)
  end

  defp encode_payload(payload), do: inspect(payload)

  defp tool_error_payload(:missing_query) do
    %{
      "error" => %{
        "message" => "`linear_graphql` requires a non-empty `query` string."
      }
    }
  end

  defp tool_error_payload(:invalid_arguments) do
    %{
      "error" => %{
        "message" => "`linear_graphql` expects either a GraphQL query string or an object with `query` and optional `variables`."
      }
    }
  end

  defp tool_error_payload(:invalid_variables) do
    %{
      "error" => %{
        "message" => "`linear_graphql.variables` must be a JSON object when provided."
      }
    }
  end

  defp tool_error_payload(:missing_linear_api_token) do
    %{
      "error" => %{
        "message" => "Symphony is missing Linear auth. Set `linear.api_key` in `WORKFLOW.md` or export `LINEAR_API_KEY`."
      }
    }
  end

  defp tool_error_payload({:linear_api_status, status}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed with HTTP #{status}.",
        "status" => status
      }
    }
  end

  defp tool_error_payload({:linear_rate_limited, details}) do
    %{
      "error" => %{
        "code" => "LINEAR_RATE_LIMITED",
        "message" => "Linear is rate limited. Do not retry this tool call; use `linear_issue_handoff` for the final state transition.",
        "retryAfterMs" => details.retry_after_ms,
        "retryAtUnixMs" => details.retry_at_unix_ms
      }
    }
  end

  defp tool_error_payload({:linear_api_request, reason}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed before receiving a successful response.",
        "reason" => inspect(reason)
      }
    }
  end

  defp tool_error_payload(reason) do
    %{
      "error" => %{
        "message" => "Linear GraphQL tool execution failed.",
        "reason" => inspect(reason)
      }
    }
  end

  defp supported_tool_names do
    Enum.map(tool_specs(), & &1["name"])
  end
end
