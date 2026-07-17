defmodule SymphonyElixir.TerminalReconciler do
  @moduledoc """
  Reconciles terminal issues owned by the current workflow lane.

  The persisted thread registry is the source of candidates. This intentionally
  does not depend on the dispatch query, which normally excludes terminal work.
  """

  require Logger

  alias SymphonyElixir.Codex.{AppServer, ThreadRegistry}
  alias SymphonyElixir.{Config, Tracker, WorkspaceRegistry}
  alias SymphonyElixir.Linear.PendingHandoff

  @batch_size 50

  @spec reconcile(map(), keyword()) :: :ok
  def reconcile(state, opts \\ []) when is_map(state) do
    registry = Keyword.get(opts, :registry, ThreadRegistry)
    tracker = Keyword.get(opts, :tracker, Tracker)
    archive_fun = Keyword.get(opts, :archive_fun, &AppServer.archive_thread/2)
    workspace_cleanup_fun = Keyword.get(opts, :workspace_cleanup_fun, &WorkspaceRegistry.cleanup/1)
    pending_fun = Keyword.get(opts, :pending_fun, &PendingHandoff.pending?/1)
    terminal_states = terminal_state_set(Keyword.get(opts, :terminal_states))

    with {:ok, entries} <- registry.entries(),
         {:ok, issues} <- fetch_in_batches(entries, tracker) do
      issues_by_id = Map.new(issues, &{&1.id, &1})

      Enum.each(entries, fn entry ->
        reconcile_entry(
          entry,
          issues_by_id,
          state,
          terminal_states,
          archive_fun,
          workspace_cleanup_fun,
          pending_fun,
          registry
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

  defp reconcile_entry(entry, issues, state, terminal_states, archive_fun, workspace_cleanup_fun, pending_fun, registry) do
    case Map.get(issues, entry.issue_id) do
      %{state: issue_state} = issue when is_binary(issue_state) ->
        cond do
          !MapSet.member?(terminal_states, normalize(issue_state)) -> :ok
          active?(state, entry.issue_id) -> log_skip(issue, entry, "active_run")
          pending_fun.(entry.issue_id) -> log_skip(issue, entry, "pending_handoff")
          true -> archive(entry, issue, archive_fun, workspace_cleanup_fun, registry)
        end

      _ ->
        Logger.warning("Terminal resource reconciliation skipped issue_id=#{entry.issue_id} lane=#{lane()} resource=thread action=archive result=preserved reason=missing_issue_metadata")
    end
  end

  defp archive(entry, issue, archive_fun, workspace_cleanup_fun, registry) do
    _ = workspace_cleanup_fun.(entry.issue_id)

    case archive_fun.(entry.thread_id, entry.worker_host) do
      :ok ->
        case registry.archive(entry.issue_id, "terminal:#{issue.state}") do
          :ok -> Logger.info("Terminal resource reconciliation issue_id=#{entry.issue_id} lane=#{lane()} resource=thread action=archive result=ok reason=terminal:#{issue.state}")
          {:error, reason} -> Logger.warning("Terminal resource reconciliation issue_id=#{entry.issue_id} lane=#{lane()} resource=thread action=archive result=retry reason=#{inspect(reason)}")
        end

      {:error, reason} ->
        Logger.warning("Terminal resource reconciliation issue_id=#{entry.issue_id} lane=#{lane()} resource=thread action=archive result=retry reason=#{inspect(reason)}")
    end
  end

  defp active?(state, issue_id) do
    Map.has_key?(Map.get(state, :running, %{}), issue_id) or
      Map.has_key?(Map.get(state, :blocked, %{}), issue_id) or
      MapSet.member?(Map.get(state, :claimed, MapSet.new()), issue_id)
  end

  defp log_skip(issue, entry, reason) do
    Logger.info("Terminal resource reconciliation issue_id=#{entry.issue_id} issue_identifier=#{issue.identifier} lane=#{lane()} resource=thread action=archive result=preserved reason=#{reason}")
  end

  defp terminal_state_set(nil), do: terminal_state_set(Config.settings!().tracker.terminal_states)
  defp terminal_state_set(states), do: states |> Enum.map(&normalize/1) |> MapSet.new()
  defp normalize(value), do: value |> String.trim() |> String.downcase()
  defp lane, do: SymphonyElixir.Workflow.workflow_file_path()
end
