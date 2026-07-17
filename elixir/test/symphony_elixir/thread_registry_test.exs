defmodule SymphonyElixir.ThreadRegistryTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Codex.ThreadRegistry

  setup do
    state_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-thread-registry-#{System.unique_integer([:positive])}"
      )

    previous_state_root = System.get_env("SYMPHONY_STATE_DIR")
    System.put_env("SYMPHONY_STATE_DIR", state_root)

    on_exit(fn ->
      restore_env("SYMPHONY_STATE_DIR", previous_state_root)
      File.rm_rf(state_root)
    end)

    %{state_root: state_root}
  end

  test "persists isolated issue thread ids across calls" do
    assert :missing = ThreadRegistry.fetch("issue-a")
    assert :ok = ThreadRegistry.put("issue-a", "thread-a", "worker-a")
    assert :ok = ThreadRegistry.put("issue-b", "thread-b")

    assert {:ok, "thread-a"} = ThreadRegistry.fetch("issue-a")
    assert {:ok, "thread-b"} = ThreadRegistry.fetch("issue-b")

    assert {:ok, %{thread_id: "thread-a", worker_host: "worker-a"}} =
             ThreadRegistry.fetch_entry("issue-a")

    assert {:ok, %{thread_id: "thread-b", worker_host: nil}} =
             ThreadRegistry.fetch_entry("issue-b")

    refute ThreadRegistry.path_for("issue-a") == ThreadRegistry.path_for("issue-b")
  end

  test "workflow lanes use separate registry namespaces" do
    original_workflow = Workflow.workflow_file_path()
    workflow_root = Path.dirname(original_workflow)
    reviewer_workflow = Path.join(workflow_root, "REVIEWER_WORKFLOW.md")
    write_workflow_file!(reviewer_workflow)

    worker_path = ThreadRegistry.path_for("issue-a")
    assert :ok = ThreadRegistry.put("issue-a", "worker-thread")

    Workflow.set_workflow_file_path(reviewer_workflow)
    reviewer_path = ThreadRegistry.path_for("issue-a")

    refute reviewer_path == worker_path
    assert :missing = ThreadRegistry.fetch("issue-a")
    assert :ok = ThreadRegistry.put("issue-a", "reviewer-thread")
    assert {:ok, "reviewer-thread"} = ThreadRegistry.fetch("issue-a")

    Workflow.set_workflow_file_path(original_workflow)
    assert {:ok, "worker-thread"} = ThreadRegistry.fetch("issue-a")
  end

  test "rejects corrupt and mismatched entries without exposing them as active mappings" do
    path = ThreadRegistry.path_for("issue-a")
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "{not-json")

    assert {:error, :invalid_registry_entry} = ThreadRegistry.fetch("issue-a")

    File.write!(
      path,
      Jason.encode!(%{
        "version" => 1,
        "issue_id" => "other-issue",
        "thread_id" => "wrong-thread"
      })
    )

    assert {:error, :invalid_registry_entry} = ThreadRegistry.fetch("issue-a")
  end

  test "atomic replacement ignores partial temporary files" do
    path = ThreadRegistry.path_for("issue-a")
    assert :ok = ThreadRegistry.put("issue-a", "thread-stable")
    File.write!(path <> ".tmp-partial", ~s({"thread_id":))

    assert {:ok, "thread-stable"} = ThreadRegistry.fetch("issue-a")
  end

  test "reports registry read and write failures", %{state_root: state_root} do
    read_path = ThreadRegistry.path_for("issue-read-error")
    File.mkdir_p!(read_path)

    assert {:error, {:registry_read_failed, :eisdir}} =
             ThreadRegistry.fetch("issue-read-error")

    blocked_root = Path.join(state_root, "blocked")
    File.write!(blocked_root, "not a directory")
    System.put_env("SYMPHONY_STATE_DIR", blocked_root)

    assert {:error, {:registry_write_failed, :enotdir}} =
             ThreadRegistry.put("issue-write-error", "thread-write-error")
  end

  test "uses the documented default state root when no override is set" do
    System.delete_env("SYMPHONY_STATE_DIR")

    assert String.starts_with?(
             ThreadRegistry.path_for("issue-default-root"),
             Path.join([System.user_home!(), ".local", "state", "symphony"])
           )
  end
end
