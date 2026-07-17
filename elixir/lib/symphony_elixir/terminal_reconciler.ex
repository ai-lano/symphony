defmodule SymphonyElixir.TerminalReconciler do
  @moduledoc """
  Reconciles terminal issues owned by the current workflow lane.

  Persisted thread and worktree registries are the candidate sources. This
  intentionally does not depend on the dispatch query, which excludes terminal
  work and would otherwise strand preserved worktrees after thread archival.
  """

  require Logger

  alias SymphonyElixir.Codex.{AppServer, ThreadRegistry}
  alias SymphonyElixir.{Config, Tracker, WorkspaceRegistry}
  alias SymphonyElixir.Linear.PendingHandoff

  @batch_size 50

  @spec reconcile(map(), keyword()) :: :ok
  def reconcile(state, opts \\ []) when is_map(state) do
    registry = Keyword.get(opts, :registry, ThreadRegistry)
    workspace_registry = Keyword.get(opts, :workspace_registry, WorkspaceRegistry)
    tracker = Keyword.get(opts, :tracker, Tracker)
    archive_fun = Keyword.get(opts, :archive_fun, &AppServer.archive_thread/2)
    workspace_cleanup_fun = Keyword.get(opts, :workspace_cleanup_fun, &WorkspaceRegistry.cleanup_result/1)
    pending_fun = Keyword.get(opts, :pending_fun, &PendingHandoff.pending?/1)
    terminal_states = terminal_state_set(Keyword.get(opts, :terminal_states))

    with {:ok, thread_entries} <- registry.entries(),
         {:ok, workspace_entries} <- workspace_registry.entries(),
         {:ok, issues} <- fetch_in_batches(thread_entries ++ workspace_entries, tracker) do
      issues_by_id = Map.new(issues, &{&1.id, &1})

      (thread_entries ++ workspace_entries)
      |> Enum.map(& &1.issue_id)
      |> Enum.uniq()
      |> Enum.each(fn issue_id ->
        reconcile_issue(
          issue_id,
          Map.get(issues_by_id, issue_id),
          Enum.find(thread_entries, &(&1.issue_id == issue_id)),
          Enum.any?(workspace_entries, &(&1.issue_id == issue_id)),
          state,
          %{
            terminal_states: terminal_states,
            archive_fun: archive_fun,
            workspace_cleanup_fun: workspace_cleanup_fun,
            pending_fun: pending_fun,
            registry: registry
          }
        )
      end)
    else
      {:error, reason} ->
        Logger.warning("Terminal resource reconciliation skipped lane=#{lane()} resource=thread action=lookup result=error reason=#{inspect(reason)}")
    end

    :ok
  end

  defp fetch_in_batches(entries, tracker) do
    entries
    |> Enum.map(& &1.issue_id)
    |> Enum.uniq()
    |> Enum.chunk_every(@batch_size)
    |> Enum.reduce_while({:ok, []}, fn ids, {:ok, issues} ->
      case tracker.fetch_issue_states_by_ids(ids) do
        {:ok, batch} -> {:cont, {:ok, issues ++ batch}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp reconcile_issue(issue_id, issue, thread_entry, workspace_present, state, context) do
    case issue do
      %{state: issue_state} = issue when is_binary(issue_state) ->
        cond do
          !MapSet.member?(context.terminal_states, normalize(issue_state)) ->
            :ok

          active?(state, issue_id) ->
            log_skip(issue, issue_id, "active_run", not is_nil(thread_entry), workspace_present)

          context.pending_fun.(issue_id) ->
            log_skip(issue, issue_id, "pending_handoff", not is_nil(thread_entry), workspace_present)

          true ->
            maybe_reconcile_worktree(workspace_present, issue, context.workspace_cleanup_fun)
            archive(thread_entry, issue, context.archive_fun, context.registry)
        end

      _ ->
        log_missing_issue_metadata(issue_id, thread_entry, workspace_present)
    end
  end

  defp archive(nil, _issue, _archive_fun, _registry), do: :ok

  defp archive(entry, issue, archive_fun, registry) do
    case archive_fun.(entry.thread_id, entry.worker_host) do
      :ok ->
        case registry.archive(entry.issue_id, "terminal:#{issue.state}") do
          :ok ->
            Logger.info(
              "Terminal resource reconciliation issue_id=#{entry.issue_id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=thread action=archive result=ok thread_id=#{entry.thread_id} reason=terminal:#{issue.state}"
            )

          {:error, reason} ->
            Logger.warning(
              "Terminal resource reconciliation issue_id=#{entry.issue_id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=thread action=archive result=retry thread_id=#{entry.thread_id} reason=#{inspect(reason)}"
            )
        end

      {:error, reason} ->
        Logger.warning(
          "Terminal resource reconciliation issue_id=#{entry.issue_id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=thread action=archive result=retry thread_id=#{entry.thread_id} reason=#{inspect(reason)}"
        )
    end
  end

  defp active?(state, issue_id) do
    Map.has_key?(Map.get(state, :running, %{}), issue_id) or
      Map.has_key?(Map.get(state, :blocked, %{}), issue_id) or
      MapSet.member?(Map.get(state, :claimed, MapSet.new()), issue_id)
  end

  defp log_skip(issue, issue_id, reason, thread_present, workspace_present) do
    if thread_present do
      Logger.info("Terminal resource reconciliation issue_id=#{issue_id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=thread action=archive result=preserved reason=#{reason}")
    end

    if workspace_present do
      Logger.info("Terminal resource reconciliation issue_id=#{issue_id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=worktree action=remove result=preserved reason=#{reason}")
    end
  end

  defp log_missing_issue_metadata(issue_id, thread_entry, workspace_present) do
    if thread_entry do
      Logger.warning(
        "Terminal resource reconciliation skipped issue_id=#{issue_id} issue_identifier=unknown lane=#{lane()} resource=thread action=archive result=preserved thread_id=#{thread_entry.thread_id} reason=missing_issue_metadata"
      )
    end

    if workspace_present do
      Logger.warning(
        "Terminal resource reconciliation skipped issue_id=#{issue_id} issue_identifier=unknown lane=#{lane()} resource=worktree action=remove result=preserved reason=missing_issue_metadata"
      )
    end
  end

  defp reconcile_worktree(issue, cleanup_fun) do
    case cleanup_fun.(issue.id) do
      {:ok, :removed} ->
        Logger.info("Terminal resource reconciliation issue_id=#{issue.id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=worktree action=remove result=ok reason=terminal")

      {:ok, :already_removed} ->
        Logger.info("Terminal resource reconciliation issue_id=#{issue.id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=worktree action=remove result=noop reason=already_removed")

      {:ok, :missing} ->
        Logger.info(
          "Terminal resource reconciliation issue_id=#{issue.id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=worktree action=remove result=noop reason=missing_registry_entry"
        )

      :ok ->
        Logger.info("Terminal resource reconciliation issue_id=#{issue.id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=worktree action=remove result=ok reason=terminal")

      {:error, reason} ->
        Logger.warning(
          "Terminal resource reconciliation issue_id=#{issue.id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=worktree action=remove result=preserved reason=#{inspect(reason)}"
        )
    end
  end

  defp maybe_reconcile_worktree(true, issue, cleanup_fun), do: reconcile_worktree(issue, cleanup_fun)
  defp maybe_reconcile_worktree(false, _issue, _cleanup_fun), do: :ok

  defp terminal_state_set(nil), do: terminal_state_set(Config.settings!().tracker.terminal_states)
  defp terminal_state_set(states), do: states |> Enum.map(&normalize/1) |> MapSet.new()
  defp normalize(value), do: value |> String.trim() |> String.downcase()
  defp lane, do: SymphonyElixir.Workflow.workflow_file_path()
end
