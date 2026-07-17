defmodule SymphonyElixir.WorkspaceRegistryTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.WorkspaceRegistry

  setup do
    root = Path.join(System.tmp_dir!(), "symphony-worktree-registry-#{System.unique_integer([:positive])}")
    repo = Path.join(root, "repo")
    remote = Path.join(root, "remote.git")
    worktree = Path.join(root, "issue")
    File.mkdir_p!(root)
    git!(root, ["init", repo])
    git!(repo, ["config", "user.email", "test@example.com"])
    git!(repo, ["config", "user.name", "Test"])
    File.write!(Path.join(repo, "README.md"), "initial\n")
    git!(repo, ["add", "README.md"])
    git!(repo, ["commit", "-m", "initial"])
    git!(repo, ["worktree", "add", "-b", "issue-branch", worktree])
    git!(root, ["init", "--bare", remote])
    git!(repo, ["remote", "add", "origin", remote])
    git!(worktree, ["push", "-u", "origin", "issue-branch"])

    on_exit(fn -> File.rm_rf(root) end)
    %{repo: repo, remote: remote, worktree: worktree}
  end

  test "removes a clean registered linked worktree and treats a repeat as a no-op", %{worktree: worktree} do
    assert :ok = WorkspaceRegistry.register("issue-1", worktree)
    assert :ok = WorkspaceRegistry.cleanup("issue-1")
    refute File.exists?(worktree)
    assert :ok = WorkspaceRegistry.cleanup("issue-1")
  end

  test "preserves a dirty registered worktree", %{worktree: worktree} do
    assert :ok = WorkspaceRegistry.register("issue-2", worktree)
    File.write!(Path.join(worktree, "dirty.txt"), "keep\n")
    assert {:error, :dirty_or_unpushed} = WorkspaceRegistry.cleanup("issue-2")
    assert File.exists?(worktree)
  end

  test "preserves a branch with no upstream or unpushed commits", %{worktree: worktree} do
    git!(worktree, ["branch", "--unset-upstream"])
    assert :ok = WorkspaceRegistry.register("issue-3", worktree)
    assert {:error, :dirty_or_unpushed} = WorkspaceRegistry.cleanup("issue-3")
    assert File.exists?(worktree)
  end

  test "preserves a worktree when its remote-tracking upstream is gone", %{repo: repo, worktree: worktree} do
    assert :ok = WorkspaceRegistry.register("issue-4", worktree)
    git!(repo, ["push", "origin", "--delete", "issue-branch"])
    git!(worktree, ["fetch", "--prune", "origin"])
    assert {:error, :missing_or_gone_upstream} = WorkspaceRegistry.cleanup("issue-4")
    assert File.exists?(worktree)
  end

  test "skips malformed metadata while retaining valid registered worktrees", %{worktree: worktree} do
    assert :ok = WorkspaceRegistry.register("issue-valid", worktree)
    File.write!(Path.join(registry_directory(), "broken.json"), "{bad")

    assert {:ok, [%{issue_id: "issue-valid"}]} = WorkspaceRegistry.entries()
  end

  defp git!(cwd, args) do
    assert {_output, 0} = System.cmd("git", args, cd: cwd, stderr_to_stdout: true)
  end

  defp registry_directory do
    workflow_key =
      Workflow.workflow_file_path()
      |> Path.expand()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    Path.join([System.fetch_env!("SYMPHONY_STATE_DIR"), "worktrees", workflow_key])
  end
end
