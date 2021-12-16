-module(coordinator).

-include("../include/task_repr.hrl").
-include("../include/data_repr.hrl").
-include("../include/coor_params.hrl").

-include_lib("kernel/include/logger.hrl").

-import(dbs, [db_to_string/1]).
-import(dl_repr, [get_rule_headname/1]).

-export([start_link/1, get_prog/1, get_static_db/1, get_num_tasks/1, assign_task/1,
         finish_task/2, stop_coordinator/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(coor_state,
        {tasks :: [mr_task()],
         num_tasks :: non_neg_integer(),
         static_db :: dl_db_instance(),
         non_static_prog :: dl_program()}).

-type state() :: #coor_state{}.

-ifdef(TEST).

-compile(export_all).

-endif.

-behaviour(gen_server).

%%% Client API
-spec start_link(string()) -> {ok, pid()}.
start_link(ProgName) ->
  net_kernel:start([?coor_name, shortnames]),
  {ok, Pid} = gen_server:start_link(?MODULE, [ProgName], []),
  global:register_name(coor, Pid),
  {ok, Pid}.

%% Synchronous call

get_num_tasks(Pid) ->
  gen_server:call(Pid, num_tasks).

get_prog(Pid) ->
  gen_server:call(Pid, prog).

get_static_db(Pid) ->
  gen_server:call(Pid, static_db).

assign_task(Pid) ->
  gen_server:call(Pid, assign).

finish_task(Pid, Task) ->
  gen_server:cast(Pid, {finish, Task}).

stop_coordinator(Pid) ->
  gen_server:call(Pid, terminate).

%%% Server functions

% there is a couple of things we need to do here
% read in rules and
-spec init([string()]) -> {ok, state()}.
init([ProgName]) ->
  Dir = "apps/erlog/test/tmp",
  case filelib:is_dir(Dir) of
    true ->
      file:del_dir_r(Dir);
    false ->
      ok
  end,
  ok = file:make_dir("apps/erlog/test/tmp"),
  {ok, Stream} = file:open("apps/erlog/test/eval_SUITE_data/" ++ ProgName, [read]),
  {Facts, Rules} = preproc:lex_and_parse(Stream),
  file:close(Stream),
  % preprocess rules
  Program = preproc:process_rules(Rules),
  ?LOG_DEBUG(#{input_prog => utils:to_string(Program)}),
  ?LOG_DEBUG(#{input_data => Facts}),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),

  % the coordinator would do a first round of evaluation to find all static relations
  NewDB = eval:imm_conseq(Program, EDB),
  FullDB = dbs:union(NewDB, EDB),
  StaticDB = FullDB,
  % this is the program that will be sent to workers
  NonStaticProg =
    lists:filter(fun(R) -> not eval:static_relation(get_rule_headname(R), Program) end,
                 Program),

  frag:hash_frag(FullDB, NonStaticProg, ?num_tasks, 1, ?inter_dir),
  Tasks = [tasks:new_task(X, 1) || X <- lists:seq(1, ?num_tasks)],
  ?LOG_DEBUG(#{tasks_after_coor_initialisation => Tasks}),
  {ok,
   #coor_state{tasks = Tasks,
               num_tasks = ?num_tasks,
               static_db = StaticDB,
               non_static_prog = NonStaticProg}}.

handle_call(prog, _From, State = #coor_state{non_static_prog = NonStaticProg}) ->
  ?LOG_DEBUG(#{assigned_prog_to_worker => utils:to_string(NonStaticProg)}),
  {reply, NonStaticProg, State};
handle_call(static_db, _From, State = #coor_state{static_db = StaticDB}) ->
  ?LOG_DEBUG(#{static_db_given_to_worker => db_to_string(StaticDB)}),
  {reply, StaticDB, State};
handle_call(num_tasks, _From, State = #coor_state{num_tasks = NumTasks}) ->
  {reply, NumTasks, State};
handle_call(assign, _From, State = #coor_state{tasks = Tasks}) ->
  {Task, NewTasks} = find_next_task(Tasks),
  ?LOG_DEBUG(#{old_tasks => Tasks, new_tasks => NewTasks}),
  {reply, Task, State#coor_state{tasks = NewTasks}};
handle_call(terminate, _From, State) ->
  {stop, normal, ok, State}.

handle_cast({finish, Task}, State = #coor_state{tasks = Tasks}) ->
  NewTasks = update_finished_task(Task, Tasks),
  {noreply, State#coor_state{tasks = NewTasks}}.

handle_info(Msg, State) ->
  io:format("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(normal, _State) ->
  ok = file:del_dir_r("apps/erlog/test/tmp"),
  io:format("coordinator terminated~n"),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%% Private functions

%%----------------------------------------------------------------------
%% @doc
%% Function: find_next_task
%% Purpose: given a list of tasks, find the next one to be assigned.
%%  if a task can be assigned that means
%%  <ol>
%%    <li>it is idle </li>
%%    <li>@todo the worker has timedout </li>
%%  </ol>
%% if there is none available, then give a wait task
%% Args:
%% @returns the task that can be assigned, and the updated list of tasks.
%% @end
%%----------------------------------------------------------------------
-spec find_next_task([mr_task()]) -> {mr_task(), [mr_task()]}.
find_next_task(Tasks) ->
  case lists:splitwith(fun(T) -> not tasks:is_idle(T) end, Tasks) of
    {NonIdles, [IdleH | IdleT]} ->
      NewTask = tasks:set_in_prog(IdleH),
      {NewTask, NonIdles ++ [NewTask | IdleT]};
    {_NonIdles, []} -> % there is no idle task in this case, return wait task
      {tasks:new_wait_task(), Tasks}
  end.

-spec update_finished_task(mr_task(), [mr_task()]) -> [mr_task()].
update_finished_task(Task, Tasks) ->
  ?LOG_DEBUG(#{task_finished => Task}),
  {L1, [L2H | L2T]} = lists:splitwith(fun(T) -> T =/= Task end, Tasks),
  L2H2 = tasks:set_finished(L2H),
  L1 ++ [L2H2 | L2T].
