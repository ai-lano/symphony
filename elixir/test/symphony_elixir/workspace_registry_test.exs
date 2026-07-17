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
    %{repo: repo, worktree: worktree}
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

  defp git!(cwd, args) do
    assert {_output, 0} = System.cmd("git", args, cd: cwd, stderr_to_stdout: true)
  end
end
