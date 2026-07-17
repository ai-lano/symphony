defmodule SymphonyElixir.TerminalReconcilerTest do
  use SymphonyElixir.TestSupport

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

  setup do
    Application.put_env(:symphony_elixir, :terminal_entries, {:ok, [%{issue_id: "terminal", thread_id: "thread-1", worker_host: nil}]})
    Application.put_env(:symphony_elixir, :terminal_issues, [%{id: "terminal", identifier: "T-1", state: "Done"}])

    on_exit(fn ->
      Application.delete_env(:symphony_elixir, :terminal_entries)
      Application.delete_env(:symphony_elixir, :terminal_issues)
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

  defp reconcile(state, overrides \\ []) do
    Orchestrator.reconcile_terminal_resources_for_test(
      state,
      Keyword.merge(
        [
          registry: RegistryStub,
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
