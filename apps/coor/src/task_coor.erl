-module(task_coor).

-behaviour(gen_statem).

-export([start_link/2, finish_task/1, request_task/2, reset_task/1]).
-export([terminate/3, init/1, callback_mode/0]).
-export([idle/3, in_progress/3, finished/3]).



-spec get_name(integer(), integer()) -> atom().
get_name(ProgNum, TaskNum) ->
  N = list_to_atom(atom_to_list(task_coor)
               ++ "-"
               ++ integer_to_list(ProgNum)
               ++ "-"
               ++ integer_to_list(TaskNum)),
  io:format("name is ~p~n", [N]),
  N.

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

idle({call, From}, {request, Pid}, Data) ->
  link(Pid),
  {next_state, in_progress, Data, {reply, From, ok}};
idle({call, From}, finish, _Data) ->
  {keep_state_and_data, {reply, From, invalid}}.

in_progress({call, From}, finish, Data) ->
  {next_state, finished, Data, {reply, From, ok}};
in_progress({call, From}, reset, Data) ->
  {next_state, idle, Data, {reply, From, ok}};
in_progress({call, From}, request, _Data) ->
  {keep_state_and_data, {reply, From, invalid}};
in_progress(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, Data).

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
  Data = #{},
  {ok, idle, Data}.

callback_mode() ->
  state_functions.

terminate(_Reason, finish, _Data) ->
  io:format("correct termination~n"),
  ok;
terminate(_Reason, _State, _Data) ->
  io:format("terminated before completion~n").


handle_event(info, {'EXIT', Pid, Reason}, Data) ->
  lager:notice("worker process ~p died with reason ~p", [Pid, Reason]),
  {next_state, idle, Data};
handle_event(EventType, EventContent, Data) ->
  lager:warning("unknown event ~p ~p", [EventType, EventContent]),
  {keep_state, Data}.

