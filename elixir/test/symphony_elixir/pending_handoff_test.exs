defmodule SymphonyElixir.Linear.PendingHandoffTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Linear.PendingHandoff

  defmodule TrackerStub do
    def update_issue_state(issue_id, state_name) do
      agent = Application.fetch_env!(:symphony_elixir, :pending_handoff_test_agent)
      recipient = Application.fetch_env!(:symphony_elixir, :pending_handoff_test_recipient)
      send(recipient, {:state_update, issue_id, state_name})

      Agent.get_and_update(agent, fn
        [result | rest] -> {result, rest}
        [] -> {:ok, []}
      end)
    end
  end

  setup do
    path = Path.join(System.tmp_dir!(), "pending-handoff-#{System.unique_integer([:positive])}.json")
    {:ok, results_agent} = Agent.start_link(fn -> [] end)
    Application.put_env(:symphony_elixir, :pending_handoff_test_agent, results_agent)
    Application.put_env(:symphony_elixir, :pending_handoff_test_recipient, self())

    on_exit(fn ->
      Application.delete_env(:symphony_elixir, :pending_handoff_test_agent)
      Application.delete_env(:symphony_elixir, :pending_handoff_test_recipient)
      if Process.alive?(results_agent), do: Agent.stop(results_agent)
      File.rm(path)
    end)

    %{path: path, results: results_agent}
  end

  test "persists before acknowledgement and removes the entry after success", %{path: path, results: results} do
    Agent.update(results, fn _ -> [:ok] end)
    name = Module.concat(__MODULE__, "Success#{System.unique_integer([:positive])}")
    start_supervised!({PendingHandoff, name: name, path: path, tracker: TrackerStub})

    assert {:ok, :queued} = PendingHandoff.enqueue("issue-1", "In Review", name)
    assert File.exists?(path)
    assert_receive {:state_update, "issue-1", "In Review"}, 1_000

    eventually(fn -> PendingHandoff.snapshot(name) == [] end)
    assert Jason.decode!(File.read!(path))["entries"] == []
  end

  test "recovers a rate-limited handoff after process restart", %{path: path, results: results} do
    rate_limited =
      {:error, {:linear_rate_limited, %{retry_after_ms: 60_000, retry_at_unix_ms: 70_000, attempt: 1, source: "test"}}}

    Agent.update(results, fn _ -> [rate_limited] end)
    first_name = Module.concat(__MODULE__, "First#{System.unique_integer([:positive])}")

    {:ok, first} =
      PendingHandoff.start_link(
        name: first_name,
        path: path,
        tracker: TrackerStub,
        now_fun: fn -> 10_000 end
      )

    assert {:ok, :queued} = PendingHandoff.enqueue("issue-2", "Todo", first_name)
    assert_receive {:state_update, "issue-2", "Todo"}, 1_000
    eventually(fn -> PendingHandoff.pending?("issue-2", first_name) end)
    assert {:ok, :queued} = PendingHandoff.enqueue("issue-2", "Todo", first_name)
    assert [%{"attempt" => 1}] = PendingHandoff.snapshot(first_name)
    GenServer.stop(first)

    assert [%{"attempt" => 1, "issue_id" => "issue-2"}] = Jason.decode!(File.read!(path))["entries"]

    Agent.update(results, fn _ -> [:ok] end)
    second_name = Module.concat(__MODULE__, "Second#{System.unique_integer([:positive])}")

    start_supervised!({PendingHandoff, name: second_name, path: path, tracker: TrackerStub, now_fun: fn -> 80_000 end})

    assert_receive {:state_update, "issue-2", "Todo"}, 1_000
    eventually(fn -> PendingHandoff.snapshot(second_name) == [] end)
  end

  test "rejects acknowledgement when the handoff cannot be persisted" do
    name = Module.concat(__MODULE__, "Unwritable#{System.unique_integer([:positive])}")

    start_supervised!({PendingHandoff, name: name, path: "/dev/null/handoffs.json", tracker: TrackerStub})

    assert {:error, {:handoff_persist_failed, _reason}} =
             PendingHandoff.enqueue("issue-3", "In Review", name)

    refute PendingHandoff.pending?("issue-3", name)
  end

  test "fails closed when the durable handoff queue is unavailable" do
    missing_name = Module.concat(__MODULE__, "Missing#{System.unique_integer([:positive])}")
    assert PendingHandoff.pending?("issue-4", missing_name)
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
