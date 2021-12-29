-module(coordinator).

-behaviour(gen_server).

-include("../include/task_repr.hrl").
-include("../include/data_repr.hrl").
-include("../include/coor_params.hrl").

-include_lib("kernel/include/logger.hrl").

-import(dl_repr, [get_rule_headname/1]).

-export([start_link/1, start_link/2, get_tmp_path/1, get_prog/1, get_num_tasks/1,
         assign_task/1, finish_task/2, finished/1, stop_coordinator/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(coor_state,
        {tasks :: [mr_task()],
         num_tasks :: non_neg_integer(),
         prog :: dl_program(),
         stage_num :: integer(),
         tmp_path :: file:filename()}).

-type state() :: #coor_state{}.

-ifdef(TEST).

-compile(export_all).

-endif.

%%% Client API
-spec start_link(file:filename()) -> {ok, pid()}.
start_link(ProgName) ->
  {ok, TmpPath} = application:get_env(erlog, inter_dir),
  start_link(ProgName, TmpPath).

-spec start_link(file:filename(), file:filename()) -> {ok, pid()}.
start_link(ProgName, TmpPath) ->
  {ok, Pid} = gen_server:start_link(?MODULE, [ProgName, TmpPath], []),
  yes = global:register_name(coor, Pid),
  {ok, Pid}.

%% Synchronous call

get_tmp_path(Pid) ->
  gen_server:call(Pid, tmp_path).

get_num_tasks(Pid) ->
  gen_server:call(Pid, num_tasks).

get_prog(Pid) ->
  gen_server:call(Pid, prog).

assign_task(Pid) ->
  gen_server:call(Pid, assign).

finish_task(Pid, Task) ->
  gen_server:cast(Pid, {finish, Task}).

stop_coordinator(Pid) ->
  gen_server:call(Pid, terminate).

finished(Pid) ->
  gen_server:call(Pid, finished).

%%% Server functions

-spec init([string()]) -> {ok, state()}.
init([ProgName, TmpPath]) ->
  % TODO is this a good place to check freshness of tmp dir
  case filelib:is_dir(TmpPath) of
    true ->
      file:del_dir_r(TmpPath);
    false ->
      ok
  end,
  ok = file:make_dir(TmpPath),
  {Facts, Rules} = preproc:lex_and_parse(file, ProgName),
  % preprocess rules
  Program = preproc:process_rules(Rules),
  ?LOG_DEBUG(#{input_prog => utils:to_string(Program)}),
  ?LOG_DEBUG(#{input_data => Facts}),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),

  % the coordinator would do a first round of evaluation to find all deltas for
  % first stage of seminaive eval
  EDBProg = eval:get_edb_program(Program),
  DeltaDB = eval:imm_conseq(EDBProg, EDB, dbs:new()),
  FullDB = dbs:union(DeltaDB, EDB),
  frag:hash_frag(FullDB, Program, 1, ?num_tasks, TmpPath ++ "fulldb"),
  frag:hash_frag(DeltaDB, Program, 1, ?num_tasks, TmpPath ++ "task"),
  Tasks = generate_one_stage_tasks(1, TmpPath),
  ?LOG_DEBUG(#{tasks_after_coor_initialisation => Tasks}),
  {ok,
   #coor_state{tasks = Tasks,
               num_tasks = ?num_tasks,
               prog = Program,
               stage_num = 1,
               tmp_path = TmpPath}}.

handle_call(tmp_path, _From, State = #coor_state{tmp_path = TmpPath}) ->
  {reply, TmpPath, State};
handle_call(prog, _From, State = #coor_state{prog = Prog}) ->
  ?LOG_DEBUG(#{assigned_prog_to_worker => utils:to_string(Prog)}),
  {reply, Prog, State};
handle_call(num_tasks, _From, State = #coor_state{num_tasks = NumTasks}) ->
  {reply, NumTasks, State};
handle_call(assign, _From, State = #coor_state{tasks = Tasks}) ->
  {Task, NewTasks} = find_next_task(Tasks),
  ?LOG_DEBUG(#{assigned_task_from_server => Task}),
  ?LOG_DEBUG(#{old_tasks => Tasks, new_tasks => NewTasks}),
  {reply, Task, State#coor_state{tasks = NewTasks}};
handle_call(finished, _From, State = #coor_state{tasks = Tasks}) ->
  case Tasks of
    [#task{type = terminate}] ->
      {reply, true, State};
    _Ts ->
      {reply, false, State}
  end;
handle_call(terminate, _From, State) ->
  {stop, normal, ok, State}.

handle_cast({finish, Task}, State = #coor_state{}) ->
  {NewTasks, SN} = update_finished_task(Task, State),
  % TODO can check NewTasks and see if we need to terminate the VM
  {noreply, State#coor_state{tasks = NewTasks, stage_num = SN}}.

handle_info(Msg, State) ->
  ?LOG_NOTICE("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(normal, _State) ->
  ?LOG_NOTICE("coordinator terminated~n"),
  init:stop(),
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
%%    <li>@todo the worker has timed out </li>
%%  </ol>
%% if there is none available, then give a wait task
%% @returns the task that can be assigned, and the updated list of tasks.
%% @end
%%----------------------------------------------------------------------
-spec find_next_task([mr_task()]) -> {mr_task(), [mr_task()]}.
find_next_task(Tasks) ->
  case lists:splitwith(fun(T) -> not tasks:is_idle(T) end, Tasks) of
    {NonIdles, [IdleH | IdleT]} ->
      NewTask = tasks:set_in_prog(IdleH),
      {NewTask, NonIdles ++ [NewTask | IdleT]};
    {[T = #task{type = terminate}], []} -> % only terminate task exists, assign it
      {T, Tasks};
    {_NonIdles, []} ->
      % there is no idle/terminate task, hence all in progress return wait task
      {tasks:new_wait_task(), Tasks}
  end.

%%----------------------------------------------------------------------
%% @doc
%% Given a task and the server state, set the given task to be finished.
%% If all tasks are finished, then generate a new list of tasks and also
%% the stage number for the next stage.
%%
%% @returns a list of new tasks, and an integer representing the stage number.
%%
%% @end
%%----------------------------------------------------------------------
-spec update_finished_task(mr_task(), state()) -> {[mr_task()], integer()}.
update_finished_task(Task,
                     #coor_state{tasks = Tasks,
                                 stage_num = SN,
                                 tmp_path = TmpPath}) ->
  ?LOG_DEBUG(#{task_finished => Task}),
  {L1, [L2H | L2T]} = lists:splitwith(fun(T) -> T =/= Task end, Tasks),
  L2H2 = tasks:set_finished(L2H),
  NewTasks = L1 ++ [L2H2 | L2T],
  case check_all_finished(NewTasks) of
    true ->
      ?LOG_DEBUG(#{finished_stage => SN}),
      case Ts = generate_one_stage_tasks(SN + 1, TmpPath) of
        [] ->
          ?LOG_DEBUG(#{evaluation_finished_at_stage => SN}),
          io:format("eval finished at stage ~p~n", [SN]),
          FinalDB = collect_results(1, TmpPath),
          io:format("final result db is ~n~s~n", [dbs:to_string(FinalDB)]),
          dbs:write_db(TmpPath ++ "final_db", FinalDB),
          {[tasks:new_terminate_task()], SN + 1};
        _Ts ->
          ?LOG_DEBUG(#{new_tasks_for_round => SN + 1, tasks => Ts}),
          {Ts, SN + 1}
      end;
    false ->
      {NewTasks, SN}
  end.

%%----------------------------------------------------------------------
%% @doc
%% Collect all results that have been generated in the full db.
%%
%% @returns the aggregated results
%%
%% @end
%%----------------------------------------------------------------------
-spec collect_results(integer(), file:filename()) -> dl_db_instance().
collect_results(TaskNum, _TmpPath) when TaskNum > ?num_tasks ->
  dbs:new();
collect_results(TaskNum, TmpPath) ->
  FileName = io_lib:format("~s-1-~w", [TmpPath ++ "fulldb", TaskNum]),
  DB = dbs:read_db(FileName),
  dbs:union(DB, collect_results(TaskNum + 1, TmpPath)).

%%----------------------------------------------------------------------
%% @doc
%% This function is used to check whether all tasks are finished, this
%% is probably not the most efficient way of checking because we need
%% to do this everytime a worker has finished a task.
%%
%% @TODO
%% <p>Alternatively, we could keep track of the number of finished tasks,
%% but we need to make sure that multiple `finish_task' calls do not
%% increment the counter. </p>
%% @end
%%----------------------------------------------------------------------
-spec check_all_finished([mr_task()]) -> boolean().
check_all_finished(Tasks) ->
  lists:all(fun tasks:is_finished/1, Tasks).

%%----------------------------------------------------------------------
%% @doc
%% We need to somehow a task at stage N has reached its fixpoint, if so,
%% then there is no need to continue, i.e. do not generate this task for
%% the next stage of evaluation.
%%
%% This function is <em>only</em> called when we are ready to go to
%% the next stage, i.e. when all tasks at this stage are finished
%% or `check_all_finished' has returned true. And so what we can do
%% is at this time, check files that are just written, and see if
%% any of them is empty/has reached fixpoint, if so, do not generate
%% the next stage task for it.
%%
%% @returns a list of tasks that are yet to be completed, and an empty
%% list if there is no task to be done.
%%
%% @end
%%----------------------------------------------------------------------
-spec generate_one_stage_tasks(integer(), file:filename()) -> [mr_task()].
generate_one_stage_tasks(StageNum, TmpPath) when StageNum > 1 ->
  lists:filtermap(fun(TaskNum) ->
                     case check_fixpoint(StageNum, TaskNum, TmpPath) of
                       true -> false; % if empty then do not generate anything
                       false -> {true, tasks:new_task(StageNum, TaskNum)}
                     end
                  end,
                  lists:seq(1, ?num_tasks));
generate_one_stage_tasks(StageNum, _TmpPath) when StageNum == 1 ->
  [tasks:new_task(StageNum, TaskNum) || TaskNum <- lists:seq(1, ?num_tasks)].

%%----------------------------------------------------------------------
%%----------------------------------------------------------------------
-spec check_fixpoint(integer(), integer(), file:filename()) -> boolean().
check_fixpoint(StageNum, TaskNum, TmpPath) ->
  FName1 = io_lib:format("~s-~w-~w", [TmpPath ++ "task", StageNum - 1, TaskNum]),
  FName2 = io_lib:format("~s-~w-~w", [TmpPath ++ "task", StageNum, TaskNum]),
  DB1 = dbs:read_db(FName1),
  DB2 = dbs:read_db(FName2),
  ?LOG_DEBUG(#{stage => StageNum,
               task_num => TaskNum,
               db1 => dbs:to_string(DB1),
               db2 => dbs:to_string(DB2),
               db_equal => dbs:equal(DB1, DB2)}),
  dbs:equal(DB1, DB2).
