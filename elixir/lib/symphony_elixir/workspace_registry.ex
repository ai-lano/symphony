defmodule SymphonyElixir.WorkspaceRegistry do
  @moduledoc """
  Persists lane-owned linked Git worktrees eligible for terminal reconciliation.

  A workspace is registered only after Git proves it is a linked worktree. This
  prevents terminal cleanup from inferring ownership from a directory name.
  """

  require Logger

  alias SymphonyElixir.Workflow

  @type entry :: %{
          issue_id: String.t(),
          workspace: Path.t(),
          common_dir: Path.t(),
          branch: String.t(),
          worker_host: String.t() | nil
        }

  @spec register(String.t() | nil, Path.t()) :: :ok | {:error, term()}
  def register(issue_id, workspace, worker_host \\ nil)

  @spec register(String.t() | nil, Path.t(), String.t() | nil) :: :ok | {:error, term()}
  def register(issue_id, workspace, nil) when is_binary(issue_id) and is_binary(workspace) do
    with {:ok, entry} <- git_entry(issue_id, workspace), do: write(entry)
  end

  def register(_issue_id, _workspace, worker_host) when is_binary(worker_host), do: {:error, :remote_workspace}
  def register(_issue_id, _workspace, _worker_host), do: {:error, :invalid_workspace_metadata}

  @spec cleanup(String.t()) :: :ok | {:error, term()}
  def cleanup(issue_id) when is_binary(issue_id) do
    case cleanup_result(issue_id) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec cleanup_result(String.t()) :: {:ok, :removed | :already_removed | :missing} | {:error, term()}
  def cleanup_result(issue_id) when is_binary(issue_id) do
    case fetch(issue_id) do
      :missing -> {:ok, :missing}
      {:ok, %{worker_host: worker_host}} when is_binary(worker_host) -> preserve(issue_id, :remote_workspace)
      {:ok, entry} -> cleanup_local(entry)
      {:error, reason} -> preserve(issue_id, reason)
    end
  end

  @spec entries() :: {:ok, [entry()]} | {:error, term()}
  def entries do
    directory = Path.dirname(path_for("registry-probe"))

    case File.ls(directory) do
      {:ok, names} -> read_entries(directory, names)
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, {:registry_read_failed, reason}}
    end
  end

  defp cleanup_local(%{workspace: workspace} = entry) do
    if File.exists?(workspace) do
      with :ok <- ensure_clean(entry),
           :ok <- ensure_live_remote_upstream(entry),
           :ok <- ensure_registered(entry),
           :ok <- ensure_expected_branch(entry),
           :ok <- run_git(entry.common_dir, ["worktree", "remove", workspace]),
           :ok <- run_git(entry.common_dir, ["worktree", "prune"]),
           :ok <- remove(entry.issue_id) do
        {:ok, :removed}
      else
        {:error, reason} -> preserve(entry.issue_id, reason)
      end
    else
      case remove(entry.issue_id) do
        :ok -> {:ok, :already_removed}
        error -> error
      end
    end
  end

  defp ensure_clean(entry) do
    with {:ok, status} <- git_output(entry.workspace, ["status", "--porcelain=v1", "--branch"]),
         true <- clean_status?(status) do
      :ok
    else
      false -> {:error, :dirty_or_unpushed}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_registered(entry) do
    with {:ok, output} <- git_output(entry.common_dir, ["worktree", "list", "--porcelain"]),
         true <- listed_and_unlocked?(output, entry.workspace) do
      :ok
    else
      false -> {:error, :unregistered_or_locked_worktree}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_live_remote_upstream(entry) do
    with {:ok, branch} <- git_output(entry.workspace, ["branch", "--show-current"]),
         branch = String.trim(branch),
         {:ok, remote} <- git_output(entry.workspace, ["config", "--get", "branch.#{branch}.remote"]),
         remote = String.trim(remote),
         {:ok, _} <- git_output(entry.workspace, ["remote", "get-url", remote]),
         {:ok, upstream} <- git_output(entry.workspace, ["rev-parse", "--abbrev-ref", "@{upstream}"]),
         upstream = String.trim(upstream),
         true <- String.starts_with?(upstream, remote <> "/"),
         {:ok, _} <- git_output(entry.workspace, ["rev-parse", "--verify", "refs/remotes/#{upstream}"]) do
      :ok
    else
      false -> {:error, :missing_or_local_upstream}
      {:error, _reason} -> {:error, :missing_or_gone_upstream}
    end
  end

  defp ensure_expected_branch(entry) do
    case git_output(entry.workspace, ["branch", "--show-current"]) do
      {:ok, branch} -> if String.trim(branch) == entry.branch, do: :ok, else: {:error, :unexpected_branch}
      {:error, reason} -> {:error, reason}
    end
  end

  defp git_entry(issue_id, workspace) do
    with {:ok, git_dir} <- git_output(workspace, ["rev-parse", "--git-dir"]),
         {:ok, common_dir} <- git_output(workspace, ["rev-parse", "--git-common-dir"]),
         {:ok, branch} <- git_output(workspace, ["branch", "--show-current"]),
         true <- linked_worktree?(workspace, git_dir, common_dir),
         true <- String.trim(branch) != "" do
      {:ok,
       %{
         issue_id: issue_id,
         workspace: Path.expand(workspace),
         common_dir: common_dir |> Path.expand(workspace) |> Path.dirname(),
         branch: String.trim(branch),
         worker_host: nil
       }}
    else
      false -> {:error, :not_linked_worktree}
      {:error, reason} -> {:error, reason}
    end
  end

  defp linked_worktree?(workspace, git_dir, common_dir) do
    Path.expand(git_dir, workspace) != Path.expand(common_dir, workspace)
  end

  defp clean_status?(status) do
    lines = String.split(status, "\n", trim: true)

    case lines do
      [branch_line] ->
        String.starts_with?(branch_line, "## ") and String.contains?(branch_line, "...") and
          !String.contains?(branch_line, ["ahead ", "behind "])

      _ ->
        false
    end
  end

  defp listed_and_unlocked?(output, workspace) do
    output
    |> String.split("\n\n", trim: true)
    |> Enum.any?(fn stanza ->
      String.contains?(stanza, "worktree #{Path.expand(workspace)}") and not String.contains?(stanza, "locked")
    end)
  end

  defp preserve(_issue_id, reason) do
    {:error, reason}
  end

  defp fetch(issue_id) do
    case File.read(path_for(issue_id)) do
      {:ok, raw} -> decode(raw, issue_id)
      {:error, :enoent} -> :missing
      {:error, reason} -> {:error, {:registry_read_failed, reason}}
    end
  end

  defp write(entry) do
    path = path_for(entry.issue_id)
    temporary = path <> ".tmp-#{System.unique_integer([:positive])}"
    payload = Jason.encode!(Map.put(entry, :version, 1))

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

  defp remove(issue_id) do
    case File.rm(path_for(issue_id)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:registry_remove_failed, reason}}
    end
  end

  defp decode(raw, issue_id) do
    case Jason.decode(raw) do
      {:ok,
       %{
         "version" => 1,
         "issue_id" => ^issue_id,
         "workspace" => workspace,
         "common_dir" => common_dir,
         "branch" => branch,
         "worker_host" => worker_host
       }}
      when is_binary(workspace) and is_binary(common_dir) and is_binary(branch) and
             (is_binary(worker_host) or is_nil(worker_host)) ->
        {:ok,
         %{
           issue_id: issue_id,
           workspace: workspace,
           common_dir: common_dir,
           branch: branch,
           worker_host: worker_host
         }}

      _ ->
        {:error, :invalid_registry_entry}
    end
  end

  defp read_entries(directory, names) do
    names
    |> Enum.filter(&String.ends_with?(&1, ".json"))
    |> Enum.reduce({:ok, []}, fn name, {:ok, entries} ->
      with {:ok, raw} <- File.read(Path.join(directory, name)),
           {:ok, entry} <- decode_any(raw) do
        {:ok, [entry | entries]}
      else
        {:error, reason} ->
          Logger.warning("Terminal resource reconciliation lane=#{lane()} resource=worktree action=read result=preserved reason=#{inspect({name, reason})}")
          {:ok, entries}
      end
    end)
    |> then(fn
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end)
  end

  defp decode_any(raw) do
    case Jason.decode(raw) do
      {:ok,
       %{
         "version" => 1,
         "issue_id" => issue_id,
         "workspace" => workspace,
         "common_dir" => common_dir,
         "branch" => branch,
         "worker_host" => worker_host
       }}
      when is_binary(issue_id) and is_binary(workspace) and is_binary(common_dir) and
             is_binary(branch) and (is_binary(worker_host) or is_nil(worker_host)) ->
        {:ok,
         %{
           issue_id: issue_id,
           workspace: workspace,
           common_dir: common_dir,
           branch: branch,
           worker_host: worker_host
         }}

      _ ->
        {:error, :invalid_registry_entry}
    end
  end

  defp git_output(cwd, args) do
    case System.cmd("git", args, cd: cwd, stderr_to_stdout: true) do
      {output, 0} -> {:ok, output}
      {output, status} -> {:error, {:git_failed, status, String.trim(output)}}
    end
  rescue
    error -> {:error, {:git_unavailable, Exception.message(error)}}
  end

  defp run_git(cwd, args) do
    case git_output(cwd, args) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp path_for(issue_id) do
    key = issue_id |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
    Path.join([state_root(), "worktrees", workflow_key(), key <> ".json"])
  end

  defp workflow_key, do: Workflow.workflow_file_path() |> Path.expand() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
  defp state_root, do: System.get_env("SYMPHONY_STATE_DIR") || Path.join([System.user_home!(), ".local", "state", "symphony"])
  defp lane, do: Workflow.workflow_file_path()
end
