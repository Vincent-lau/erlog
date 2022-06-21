-module(task_coor).

-behaviour(gen_statem).

-export([start_link/2, finish_task/1, request_task/2, reset_task/1]).
-export([terminate/3, init/1, callback_mode/0]).
-export([idle/3, in_progress/3, finished/3]).

-record(state, {worker_pid :: pid() | undefined}).

-spec get_name(integer(), integer()) -> atom().
get_name(ProgNum, TaskNum) ->
  list_to_atom(atom_to_list(task_coor)
               ++ "-"
               ++ integer_to_list(ProgNum)
               ++ "-"
               ++ integer_to_list(TaskNum)).

start_link(ProgNum, TaskNum) ->
  gen_statem:start_link({local, get_name(ProgNum, TaskNum)}, ?MODULE, [], []).

-spec finish_task(gen_statem:server_ref()) -> ok | invalid.
finish_task(ServerRef) ->
  gen_statem:call(ServerRef, finish).

-spec request_task(gen_statem:server_ref(), pid()) -> ok | invalid.
request_task(ServerRef, Pid) ->
  gen_statem:call(ServerRef, {request, Pid}).

-spec reset_task(gen_statem:server_ref()) -> ok | invalid.
reset_task(ServerRef) ->
  gen_statem:call(ServerRef, reset).

idle({call, From}, {request, Pid}, Data = #state{}) ->
  link(Pid),
  {next_state, in_progress, Data#state{worker_pid = Pid}, {reply, From, ok}};
idle({call, From}, finish, _Data) ->
  {keep_state_and_data, {reply, From, invalid}}.

in_progress({call, From}, finish, Data = #state{worker_pid = Pid}) ->
  unlink(Pid),
  {next_state, finished, Data#state{worker_pid = undefined}, {reply, From, ok}};
in_progress({call, From}, reset, Data = #state{worker_pid = Pid}) ->
  unlink(Pid),
  {next_state, idle, Data#state{worker_pid = undefined}, {reply, From, ok}};
in_progress({call, From}, request, _Data) ->
  {keep_state_and_data, {reply, From, invalid}};
in_progress(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, Data).

% TODO add timeout

finished({call, From}, finish, _Data) ->
  % duplicate finish message
  {keep_state_and_data, {reply, From, ok}};
finished({call, From}, _State, _Data) ->
  % we have a request or reset, might want to allow reset finished tasks
  {keep_state_and_data, {reply, From, invalid}};
finished(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, Data).

init([]) ->
  process_flag(trap_exit, true),
  Data = #state{worker_pid = undefined},
  {ok, idle, Data}.

callback_mode() ->
  state_functions.

terminate(_Reason, finish, _Data) ->
  lager:debug("correct termination~n"),
  ok;
terminate(Reason, State, _Data) ->
  lager:debug("terminated with reason ~p and state ~p", [Reason, State]).

handle_event(info, {'EXIT', Pid, Reason}, Data) ->
  % the worker process has exited
  lager:notice("worker process ~p died with reason ~p", [Pid, Reason]),
  ok = coordinator:reset_worker(Pid),
  {next_state, idle, Data#state{worker_pid = undefined}};
handle_event(EventType, EventContent, Data) ->
  lager:warning("unknown event ~p ~p", [EventType, EventContent]),
  {keep_state, Data}.
