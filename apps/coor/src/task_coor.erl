-module(task_coor).

-behaviour(gen_statem).

-compile(export_all).


-spec get_name(integer()) -> atom().
get_name(TaskNum) ->
  list_to_atom(atom_to_list(task_coor) ++ integer_to_list(TaskNum)).


start_link(TaskNum) ->
  gen_statem:start_link({local, get_name(TaskNum)}, ?MODULE, TaskNum, []).



finish_task(ServerRef) -> 
  gen_statem:call(ServerRef, finish).
request_task(ServerRef) -> 
  gen_statem:call(ServerRef, request).
reset_task(ServerRef) ->
  gen_statem:call(ServerRef, reset).



idle({call, From}, request, Data) ->
  % TODO link to the worker to track status of worker, remotely link??
  {next_state, in_progress, Data, {reply, From, ok}};
idle({call, From}, finish, _Data) ->
  {keep_state_and_data, {reply, From, invalid}}.

in_progress({call, From}, finish, Data) ->
  {next_state, finished, Data, {reply, From, ok}};
in_progress({call, From}, reset, Data) ->
  {next_state, idle, Data, {reply, From, ok}};
in_progress({call, From}, request, _Data) ->
  % TODO more complex decision making involves the timing info
  {keep_state_and_data, {reply, From, invalid}}.

finished({call, From}, finish, _Data) ->
  % duplicate finish message
  {keep_state_and_data, {reply, From, ok}};
finished({call, From}, _State, _Data) -> 
  % we have a request or reset, might want to allow reset finished tasks
  {keep_state_and_data, {reply, From, invalid}}.


init(TaskNum) ->
  io:format("starting state machine for task ~p~n", [TaskNum]),
  Data = #{},
  {ok, idle, Data}.

callback_mode() ->
  state_functions.


terminate(_Reason, finish, _Data) ->
  io:format("correct termination~n"),
  ok;
terminate(_Reason, _State, _Data) ->
  io:format("terminated before completion~n").
