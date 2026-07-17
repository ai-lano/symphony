defmodule SymphonyElixir.Linear.PendingHandoff do
  @moduledoc """
  Durable, retryable Linear issue-state handoffs.

  Entries are written atomically before the caller is acknowledged. A pending
  issue remains reserved from agent dispatch until its idempotent state update
  succeeds, including across Symphony restarts.
  """

  use GenServer
  require Logger

  alias SymphonyElixir.Linear.RateLimiter
  alias SymphonyElixir.{Tracker, Workflow}

  @retry_base_ms 10_000
  @retry_max_ms 300_000

  @type entry :: %{
          required(String.t()) => term()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec enqueue(String.t(), String.t(), GenServer.name()) :: {:ok, :queued} | {:error, term()}
  def enqueue(issue_id, state_name, server \\ __MODULE__)
      when is_binary(issue_id) and is_binary(state_name) do
    GenServer.call(server, {:enqueue, String.trim(issue_id), String.trim(state_name)})
  catch
    :exit, reason -> {:error, {:handoff_queue_unavailable, reason}}
  end

  @spec pending?(String.t(), GenServer.name()) :: boolean()
  def pending?(issue_id, server \\ __MODULE__) when is_binary(issue_id) do
    GenServer.call(server, {:pending?, issue_id})
  catch
    :exit, _reason -> true
  end

  @spec snapshot(GenServer.name()) :: [entry()]
  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  catch
    :exit, _reason -> []
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, default_path())
    entries = load_entries(path)

    state = %{
      path: path,
      entries: entries,
      timer_ref: nil,
      tracker: Keyword.get(opts, :tracker, Tracker),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.system_time(:millisecond) end),
      jitter_fun: Keyword.get(opts, :jitter_fun, &:rand.uniform/1)
    }

    {:ok, schedule_drain(state, 0)}
  end

  @impl true
  def handle_call({:enqueue, "", _state_name}, _from, state),
    do: {:reply, {:error, :invalid_issue_id}, state}

  def handle_call({:enqueue, _issue_id, ""}, _from, state),
    do: {:reply, {:error, :invalid_state_name}, state}

  def handle_call({:enqueue, issue_id, state_name}, _from, state) do
    now_ms = state.now_fun.()

    entry =
      case Map.get(state.entries, issue_id) do
        %{"state_name" => ^state_name} = existing ->
          existing

        _ ->
          %{
            "issue_id" => issue_id,
            "state_name" => state_name,
            "attempt" => 0,
            "created_at_unix_ms" => now_ms,
            "next_retry_at_unix_ms" => now_ms,
            "last_error" => nil
          }
      end

    entries = Map.put(state.entries, issue_id, entry)

    case persist_entries(state.path, entries) do
      :ok ->
        Logger.info("Queued durable Linear handoff issue_id=#{issue_id} target_state=#{state_name}")
        {:reply, {:ok, :queued}, schedule_drain(%{state | entries: entries}, 0)}

      {:error, reason} ->
        {:reply, {:error, {:handoff_persist_failed, reason}}, state}
    end
  end

  def handle_call({:pending?, issue_id}, _from, state) do
    {:reply, Map.has_key?(state.entries, issue_id), state}
  end

  def handle_call(:snapshot, _from, state) do
    {:reply, Map.values(state.entries), state}
  end

  @impl true
  def handle_info(:drain, state) do
    state = %{state | timer_ref: nil}
    now_ms = state.now_fun.()

    case next_due_entry(state.entries, now_ms) do
      nil ->
        {:noreply, schedule_next_entry(state, now_ms)}

      entry ->
        {:noreply, entry |> attempt_handoff(state, now_ms) |> schedule_drain(0)}
    end
  end

  defp attempt_handoff(entry, state, now_ms) do
    issue_id = entry["issue_id"]
    state_name = entry["state_name"]

    case state.tracker.update_issue_state(issue_id, state_name) do
      :ok ->
        entries = Map.delete(state.entries, issue_id)
        :ok = persist_entries(state.path, entries)
        Logger.info("Completed durable Linear handoff issue_id=#{issue_id} target_state=#{state_name}")
        %{state | entries: entries}

      {:error, reason} ->
        attempt = entry["attempt"] + 1
        delay_ms = RateLimiter.retry_after_ms(reason) || retry_delay(attempt, state.jitter_fun)

        updated = %{
          entry
          | "attempt" => attempt,
            "next_retry_at_unix_ms" => now_ms + delay_ms,
            "last_error" => inspect(reason)
        }

        entries = Map.put(state.entries, issue_id, updated)
        :ok = persist_entries(state.path, entries)

        Logger.warning(
          "Durable Linear handoff pending issue_id=#{issue_id} target_state=#{state_name} " <>
            "retry_after_ms=#{delay_ms} attempt=#{attempt} error=#{inspect(reason)}"
        )

        %{state | entries: entries}
    end
  end

  defp retry_delay(attempt, jitter_fun) do
    power = min(max(attempt - 1, 0), 10)
    delay = min(@retry_base_ms * Integer.pow(2, power), @retry_max_ms)
    spread = max(div(delay, 5), 1)
    delay - spread + jitter_fun.(spread * 2 + 1) - 1
  end

  defp next_due_entry(entries, now_ms) do
    entries
    |> Map.values()
    |> Enum.filter(&(&1["next_retry_at_unix_ms"] <= now_ms))
    |> Enum.min_by(&{&1["next_retry_at_unix_ms"], &1["created_at_unix_ms"]}, fn -> nil end)
  end

  defp schedule_next_entry(%{entries: entries} = state, _now_ms) when map_size(entries) == 0,
    do: cancel_timer(state)

  defp schedule_next_entry(%{entries: entries} = state, now_ms) do
    next_at = entries |> Map.values() |> Enum.map(& &1["next_retry_at_unix_ms"]) |> Enum.min()
    schedule_drain(state, max(0, next_at - now_ms))
  end

  defp schedule_drain(state, delay_ms) do
    state = cancel_timer(state)
    %{state | timer_ref: Process.send_after(self(), :drain, delay_ms)}
  end

  defp cancel_timer(%{timer_ref: timer_ref} = state) when is_reference(timer_ref) do
    Process.cancel_timer(timer_ref)
    %{state | timer_ref: nil}
  end

  defp cancel_timer(state), do: state

  defp default_path do
    root =
      System.get_env("SYMPHONY_STATE_DIR") ||
        Path.join([System.user_home!(), ".local", "state", "symphony"])

    workflow_key =
      Workflow.workflow_file_path()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    Path.join([root, "handoffs", workflow_key <> ".json"])
  end

  defp load_entries(path) do
    with {:ok, raw} <- File.read(path),
         {:ok, %{"entries" => entries}} when is_list(entries) <- Jason.decode(raw) do
      Map.new(entries, &{&1["issue_id"], &1})
    else
      _ -> %{}
    end
  end

  defp persist_entries(path, entries) do
    temporary = path <> ".tmp-#{System.unique_integer([:positive])}"
    payload = Jason.encode!(%{"version" => 1, "entries" => Map.values(entries)})

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary, payload),
         :ok <- File.rename(temporary, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary)
        {:error, reason}
    end
  end
end
