defmodule SymphonyElixir.Codex.ThreadRegistry do
  @moduledoc """
  Persists one Codex thread id per issue for the current workflow lane.

  Each issue uses its own atomically replaced file so concurrent issues do not
  contend on a shared read-modify-write registry.
  """

  alias SymphonyElixir.Workflow

  @type fetch_result :: {:ok, String.t()} | :missing | {:error, term()}

  @spec fetch(String.t()) :: fetch_result()
  def fetch(issue_id) when is_binary(issue_id) do
    path = path_for(issue_id)

    case File.read(path) do
      {:ok, raw} ->
        decode(raw, issue_id)

      {:error, :enoent} ->
        :missing

      {:error, reason} ->
        {:error, {:registry_read_failed, reason}}
    end
  end

  @spec put(String.t(), String.t()) :: :ok | {:error, term()}
  def put(issue_id, thread_id) when is_binary(issue_id) and is_binary(thread_id) do
    path = path_for(issue_id)
    temporary = path <> ".tmp-#{System.unique_integer([:positive])}"

    payload =
      Jason.encode!(%{
        "version" => 1,
        "issue_id" => issue_id,
        "thread_id" => thread_id
      })

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary, payload),
         :ok <- File.rename(temporary, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary)
        {:error, {:registry_write_failed, reason}}
    end
  end

  @doc false
  @spec path_for(String.t()) :: Path.t()
  def path_for(issue_id) when is_binary(issue_id) do
    issue_key =
      issue_id
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    Path.join([state_root(), "codex_threads", workflow_key(), issue_key <> ".json"])
  end

  defp decode(raw, expected_issue_id) do
    case Jason.decode(raw) do
      {:ok,
       %{
         "version" => 1,
         "issue_id" => ^expected_issue_id,
         "thread_id" => thread_id
       }}
      when is_binary(thread_id) and byte_size(thread_id) > 0 ->
        {:ok, thread_id}

      _ ->
        {:error, :invalid_registry_entry}
    end
  end

  defp workflow_key do
    Workflow.workflow_file_path()
    |> Path.expand()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp state_root do
    System.get_env("SYMPHONY_STATE_DIR") ||
      Path.join([System.user_home!(), ".local", "state", "symphony"])
  end
end
