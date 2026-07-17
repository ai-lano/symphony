defmodule SymphonyElixir.TerminalReconcilerTest do
  use SymphonyElixir.TestSupport

  import ExUnit.CaptureLog

  alias SymphonyElixir.Orchestrator

  defmodule RegistryStub do
    def entries, do: Application.fetch_env!(:symphony_elixir, :terminal_entries)
    def archive(issue_id, reason), do: send(self(), {:archived, issue_id, reason}) && :ok
  end

  defmodule TrackerStub do
    def fetch_issue_states_by_ids(ids) do
      send(self(), {:fetched, ids})
      {:ok, Application.fetch_env!(:symphony_elixir, :terminal_issues)}
    end
  end

  defmodule WorkspaceRegistryStub do
    def entries, do: Application.fetch_env!(:symphony_elixir, :terminal_workspaces)
  end

  setup do
    Application.put_env(:symphony_elixir, :terminal_entries, {:ok, [%{issue_id: "terminal", thread_id: "thread-1", worker_host: nil}]})
    Application.put_env(:symphony_elixir, :terminal_issues, [%{id: "terminal", identifier: "T-1", state: "Done"}])
    Application.put_env(:symphony_elixir, :terminal_workspaces, {:ok, []})

    on_exit(fn ->
      Application.delete_env(:symphony_elixir, :terminal_entries)
      Application.delete_env(:symphony_elixir, :terminal_issues)
      Application.delete_env(:symphony_elixir, :terminal_workspaces)
    end)
  end

  test "archives a terminal lane-owned thread and removes its active mapping" do
    assert :ok = reconcile(%Orchestrator.State{})
    assert_received {:fetched, ["terminal"]}
    assert_received {:workspace_cleanup, "terminal"}
    assert_received {:archive_call, "thread-1", nil}
    assert_received {:archived, "terminal", "terminal:Done"}
  end

  test "keeps non-terminal mappings" do
    Application.put_env(:symphony_elixir, :terminal_issues, [%{id: "terminal", identifier: "T-1", state: "In Progress"}])
    assert :ok = reconcile(%Orchestrator.State{})
    refute_received {:archive_call, _, _}
    refute_received {:archived, _, _}
  end

  test "keeps mappings while a run or handoff is active" do
    state = %Orchestrator.State{running: %{"terminal" => %{}}}
    assert :ok = reconcile(state)
    refute_received {:archive_call, _, _}

    assert :ok = reconcile(%Orchestrator.State{}, pending_fun: fn _ -> true end)
    refute_received {:archive_call, _, _}
  end

  test "retains the mapping after an archive failure so a later pass retries" do
    assert :ok = reconcile(%Orchestrator.State{}, archive_fun: fn _, _ -> {:error, :offline} end)
    refute_received {:archived, _, _}

    assert :ok = reconcile(%Orchestrator.State{})
    assert_received {:archived, "terminal", "terminal:Done"}
  end

  test "retries a registered worktree after its thread mapping has archived" do
    Application.put_env(:symphony_elixir, :terminal_entries, {:ok, []})
    Application.put_env(:symphony_elixir, :terminal_workspaces, {:ok, [%{issue_id: "terminal"}]})

    assert :ok = reconcile(%Orchestrator.State{})
    assert_received {:workspace_cleanup, "terminal"}
    refute_received {:archive_call, _, _}
  end

  test "logs a preserved registered worktree when an active run blocks cleanup" do
    Application.put_env(:symphony_elixir, :terminal_entries, {:ok, []})
    Application.put_env(:symphony_elixir, :terminal_workspaces, {:ok, [%{issue_id: "terminal"}]})

    log = capture_log(fn -> assert :ok = reconcile(%Orchestrator.State{running: %{"terminal" => %{}}}) end)

    assert log =~ "issue_id=terminal"
    assert log =~ "issue_identifier=T-1"
    assert log =~ "resource=worktree"
    assert log =~ "action=remove"
    assert log =~ "result=preserved"
    assert log =~ "reason=active_run"
  end

  test "logs typed registered worktree cleanup outcomes" do
    Application.put_env(:symphony_elixir, :terminal_entries, {:ok, []})
    Application.put_env(:symphony_elixir, :terminal_workspaces, {:ok, [%{issue_id: "terminal"}]})

    log =
      capture_log(fn ->
        assert :ok = reconcile(%Orchestrator.State{}, workspace_cleanup_fun: fn _ -> {:ok, :already_removed} end)
      end)

    assert log =~ "issue_id=terminal"
    assert log =~ "issue_identifier=T-1"
    assert log =~ "resource=worktree"
    assert log =~ "action=remove"
    assert log =~ "result=noop"
    assert log =~ "reason=already_removed"
  end

  defp reconcile(state, overrides \\ []) do
    Orchestrator.reconcile_terminal_resources_for_test(
      state,
      Keyword.merge(
        [
          registry: RegistryStub,
          workspace_registry: WorkspaceRegistryStub,
          tracker: TrackerStub,
          terminal_states: ["Done"],
          pending_fun: fn _ -> false end,
          archive_fun: fn thread_id, worker_host ->
            send(self(), {:archive_call, thread_id, worker_host})
            :ok
          end,
          workspace_cleanup_fun: fn issue_id ->
            send(self(), {:workspace_cleanup, issue_id})
            :ok
          end
        ],
        overrides
      )
    )
  end
end
