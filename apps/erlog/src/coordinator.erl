-module(coordinator).

-behaviour(gen_server).

-include("../include/task_repr.hrl").

-include_lib("kernel/include/logger.hrl").

-import(dl_repr, [get_rule_headname/1]).

-export([start_link/3, start_link/2, start_link/1, get_num_tasks/0,
         get_current_stage_num/0, assign_task/1, finish_task/3, done/0, stop/0, reg_worker/1,
         get_full_db/0, get_delta_db/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([collect_results/3, collect_results/4, wait_for_finish/1]).

-record(coor_state,
        {tasks :: [mr_task()],
         num_tasks :: integer(),
         programs :: [dl_program()],
         prog_num :: integer(),
         stage_num :: integer(),
         tmp_path :: file:filename(),
         task_timeout :: timeout(),
         full_pair :: {dl_db_instance(), dl_db_instance()},
         delta_pair :: {dl_db_instance(), dl_db_instance()},
         nodes_rate :: #{node() => number()},
         nodes_tasks :: #{node() => mr_task()}}).

        % use task/file size divide execution time, exp avg to represent speed
        % paper uses ProgressScore/T to represent ProgressRate, here we do something
        % similar, just replacing ProgressScore with file size

-type state() :: #coor_state{}.

-ifdef(TEST).

-compile(export_all).

-endif.

-define(INITIAL_TIMEOUT, 240 * 1000).
% higher alpha discounts older observations faster
-define(TIMEOUT_ALPHA, 0.5).
-define(COMPLETION_THRESHOLD, 0.9).
-define(INITIAL_RATE, 0.001).

-spec avg_timeout(timer:time(), timer:time()) -> timer:time().
avg_timeout(_TimeTaken, infinity) ->
  infinity;
avg_timeout(TimeTaken, CurTimeOut) ->
  % adding some leeway to tolerate slightly slower worker(s)
  exp_avg(TimeTaken, CurTimeOut) + 3 * 1000.

-spec exp_avg(number(), number()) -> number().
exp_avg(NewNum, CurNum) ->
  ?TIMEOUT_ALPHA * NewNum + (1 - ?TIMEOUT_ALPHA) * CurNum.

% currently no backoff_timeout due to existence of stragglers affecting execution time
-spec backoff_timeout(timeout()) -> timeout().
backoff_timeout(TimeOut) ->
  TimeOut.

name() ->
  coor.

%%% Client API
-spec start_link(file:filename()) -> {ok, pid()}.
start_link(ProgName) ->
  {ok, TmpPath} = application:get_env(erlog, inter_dir),
  start_link(ProgName, TmpPath).

-spec start_link(file:filename(), file:filename()) -> {ok, pid()}.
start_link(ProgName, TmpPath) ->
  {ok, NumTasks} = application:get_env(erlog, num_tasks),
  start_link(ProgName, TmpPath, NumTasks).

-spec start_link(file:filename(), file:filename(), integer()) -> {ok, pid()}.
start_link(ProgName, TmpPath, NumTasks) ->
  gen_server:start_link({global, name()}, ?MODULE, [ProgName, TmpPath, NumTasks], []).

%% Synchronous call

reg_worker(WorkerNode) ->
  gen_server:cast({global, name()}, {reg, WorkerNode}).

get_current_stage_num() ->
  gen_server:call({global, name()}, stage_num).

get_prog_num() ->
  gen_server:call({global, name()}, prog_num).

get_full_db() ->
  gen_server:call({global, name()}, full_db).

get_delta_db() ->
  gen_server:call({global, name()}, delta_db).

get_num_tasks() ->
  gen_server:call({global, name()}, num_tasks).

assign_task(WorkerNode) ->
  gen_server:call({global, name()}, {assign, WorkerNode}).

finish_task(Task, WorkerNode, {FullDB, DeltaDB}) ->
  gen_server:call({global, name()}, {finish, Task, WorkerNode, {FullDB, DeltaDB}}).

stop() ->
  gen_server:call({global, name()}, terminate).

done() ->
  gen_server:call({global, name()}, finished).

%%% Server functions

-spec init([string()]) -> {ok, state()}.
init([ProgName, TmpPath, NumTasks]) ->
  % we check the freshness of tmp just in case
  {Facts, Rules} = preproc:lex_and_parse(file, ProgName),
  % preprocess rules
  Program = preproc:process_rules(Rules),
  Programs = neg:compute_stratification(anyorder, Program),
  ?LOG_DEBUG(#{input_prog => dl_repr:program_to_string(Program)}),
  ?LOG_DEBUG(#{input_data => Facts}),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),

  Tasks = program_to_tasks(EDB, hd(Programs), 1, NumTasks),
  {FullDB, DeltaDB} = program_to_dbs(EDB, hd(Programs)),
  lager:debug("tasks_after_coor_initialisation ~p", [Tasks]),

  {ok,
   #coor_state{tasks = Tasks,
               num_tasks = NumTasks,
               programs = Programs,
               tmp_path = TmpPath,
               prog_num = 1,
               stage_num = 1,
               full_pair = {FullDB, dbs:new()},
               delta_pair = {DeltaDB, dbs:new()},
               task_timeout = ?INITIAL_TIMEOUT,
               nodes_rate = maps:new(),
               nodes_tasks = maps:new()}}.

handle_call(stage_num, _From, State = #coor_state{stage_num = StageNum}) ->
  {reply, StageNum, State};
handle_call(prog_num, _From, State = #coor_state{prog_num = ProgNum}) ->
  {reply, ProgNum, State};
handle_call(full_db, _From, State = #coor_state{full_pair = {CurFull, _NextFull}}) ->
  {reply, CurFull, State};
handle_call(delta_db, _From, State = #coor_state{delta_pair = {CurDelta, _NextDelta}}) ->
  {reply, CurDelta, State};
handle_call(num_tasks, _From, State = #coor_state{num_tasks = NumTasks}) ->
  {reply, NumTasks, State};
handle_call({assign, WorkerNode},
            _From,
            State = #coor_state{tasks = Tasks, nodes_tasks = NodesTasks}) ->
  case tasks:is_eval(
         maps:get(WorkerNode, NodesTasks))
  of
    true -> % if assigned an eval task, then give it a wait one
      {reply, maps:get(WorkerNode, NodesTasks), State};
    false ->
      {Task, NewState} = find_next_task(WorkerNode, State),
      ?LOG_DEBUG(#{assigned_task_from_server => Task, to => WorkerNode}),
      ?LOG_DEBUG(#{old_tasks => Tasks, new_tasks => NewState#coor_state.tasks}),
      {reply, Task, NewState#coor_state{nodes_tasks = NodesTasks#{WorkerNode => Task}}}
  end;
handle_call({finish, Task, WorkerNode, {FullDB, DeltaDB}},
            _From,
            State = #coor_state{nodes_tasks = NodesTasks}) ->
  NewState = update_finished_task(Task, {FullDB, DeltaDB}, State),
  {reply,
   ok,
   NewState#coor_state{nodes_tasks = NodesTasks#{WorkerNode => tasks:new_wait_task()}}};
handle_call(finished, _From, State = #coor_state{tasks = Tasks}) ->
  case Tasks of
    [#task{type = terminate}] ->
      {reply, true, State};
    _Ts ->
      {reply, false, State}
  end;
handle_call(terminate, _From, State) ->
  {stop, normal, ok, State}.

handle_cast({reg, WorkerNode},
            State = #coor_state{nodes_rate = NodesRate, nodes_tasks = NodesTasks}) ->
  NewNodesRate = NodesRate#{WorkerNode => ?INITIAL_RATE}, % to avoid division by zero
  NewNodesTasks = NodesTasks#{WorkerNode => tasks:new_wait_task()},
  {noreply, State#coor_state{nodes_rate = NewNodesRate, nodes_tasks = NewNodesTasks}}.

handle_info(Msg, State) ->
  ?LOG_NOTICE("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(normal, #coor_state{tmp_path = TmpPath}) ->
  clean_tmp(TmpPath),
  lager:debug("coordinator terminated~n", []),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%% Private functions

-spec clean_tmp(file:filename()) -> ok.
clean_tmp(TmpPath) ->
  case filelib:is_dir(TmpPath) of
    true ->
      file:del_dir_r(TmpPath);
    false ->
      ok
  end.

-spec timed_out_task(mr_task(), timeout()) -> boolean().
timed_out_task(#task{state = in_progress, start_time = StartTime}, TimeOut) ->
  erlang:monotonic_time(millisecond) - StartTime > TimeOut;
timed_out_task(_T,
               _TimeOut) -> % if the task is not in progress, then it has not timed out
  false.

%% @doc this function will find an idle task, and if found, set its state appropriately
-spec find_set_idle_task([mr_task()], node()) -> {mr_task(), [mr_task()]} | none.
find_set_idle_task(Tasks, WorkerNode) ->
  case lists:splitwith(fun(T) -> not tasks:is_idle(T) end, Tasks) of
    {NonIdle, [IdleH | IdleT]} ->
      NewTask = tasks:set_in_prog(IdleH),
      AssignedTask = tasks:set_worker(NewTask, WorkerNode),
      {AssignedTask, NonIdle ++ [AssignedTask | IdleT]};
    {_NonIdle, []} ->
      none
  end.

-spec find_set_timed_out_task([mr_task()], node(), timeout()) ->
                               {mr_task(), [mr_task()]} | none.
find_set_timed_out_task(Tasks, WorkerNode, TimeOut) ->
  case lists:splitwith(fun(T) -> not timed_out_task(T, TimeOut) end, Tasks) of
    {TimedIn, [TimedOutH | TimedOutT]} ->
      NewTask = tasks:reset_time(TimedOutH),
      AssignedTask = tasks:set_worker(NewTask, WorkerNode),
      {AssignedTask, TimedIn ++ [AssignedTask | TimedOutT]};
    {_TimedIn, []} ->
      none
  end.

-spec possibly_faster(mr_task(), number(), number()) -> boolean().
possibly_faster(#task{size = Size,
                      start_time = StartTime,
                      state = in_progress},
                AssignedWorkerRate,
                NewWorkerRate) ->
  EstCompletion = Size / NewWorkerRate,
  EstRemaining =
    Size / AssignedWorkerRate - (erlang:monotonic_time(millisecond) - StartTime),
  % io:format("estimated remaining time is~p~n", [EstRemaining]),
  case EstRemaining < 0 of
    true ->
      true;
    false ->
      EstCompletion < EstRemaining
  end;
possibly_faster(_T, _A, _N) ->
  false.

-spec find_set_speculative_task([mr_task()], node(), #{node() => number()}) ->
                                 {mr_task(), [mr_task()]} | none.
find_set_speculative_task(Tasks, WorkerNode, NodesRate) ->
  % for each task, calculate the estimated time, compare with estimated remaining time
  case lists:splitwith(fun(T) ->
                          not
                            possibly_faster(T,
                                            maps:get(T#task.assigned_worker, NodesRate),
                                            maps:get(WorkerNode, NodesRate))
                       end,
                       Tasks)
  of
    {DefSlower, [PosFasterH | PosFasterT]} ->
      NewTask = tasks:reset_time(PosFasterH),
      AssignedTask = tasks:set_worker(NewTask, WorkerNode),
      {AssignedTask, DefSlower ++ [AssignedTask | PosFasterT]};
    {_DefSlower, []} ->
      none
  end.

-spec finished_percent([mr_task()]) -> float().
finished_percent(Tasks) ->
  length(lists:filter(fun tasks:is_finished/1, Tasks)) / length(Tasks).

-spec near_completion([mr_task()]) -> boolean().
near_completion(Tasks) ->
  finished_percent(Tasks) > ?COMPLETION_THRESHOLD.

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
%% @param WorkerNode is the node that is requesting the task
%% @returns the task that can be assigned, and the updated list of tasks.
%% @end
%%----------------------------------------------------------------------
-spec find_next_task(node(), state()) -> {mr_task(), state()}.
find_next_task(WorkerNode,
               State =
                 #coor_state{tasks = Tasks1,
                             nodes_rate = NodesRate,
                             nodes_tasks = NodesTasks}) ->
  % We reset the task here instead of spawn a new process and periodically
  % ping the worker because in that way, we would have to use msg passing
  % to send back the modified Tasks, and this can be cumbersome, since we would
  % have to put the logic to update the state of the server somewhere anyway,
  % where do we put it, and how often do we check that, and also how do we make
  % sure that the task sent to us is up to date? I don't see much value in doing
  % that.
  Tasks = reset_dead_tasks(Tasks1),
  case Tasks of
    [T = #task{type = terminate}] -> % only terminate task exists, assign it
      {T, State#coor_state{tasks = Tasks}};
    _Other ->
      case find_set_idle_task(Tasks, WorkerNode) % then we look for idle tasks
      of
        {AssignedTask, NewTasks} ->
          NewNodesTasks = NodesTasks#{WorkerNode => AssignedTask},
          {AssignedTask, State#coor_state{tasks = NewTasks, nodes_tasks = NewNodesTasks}};
        none ->
          % case find_timed_out_task(Tasks, WorkerNode, TimeOut) % now we look for timed out tasks
          % of
          %   {AssignedTask, NewTasks} ->
          %     {AssignedTask,
          %      State#coor_state{tasks = NewTasks, task_timeout = backoff_timeout(TimeOut)}};
          case near_completion(Tasks) of
            true ->
              case find_set_speculative_task(Tasks,
                                             WorkerNode,
                                             NodesRate) % finally go for backup tasks
              of
                {AssignedTask, NewTasks} ->
                  ?LOG_DEBUG(#{reassigning_task => AssignedTask,
                               from => AssignedTask#task.assigned_worker,
                               to => WorkerNode}),
                  NewNodesTasks = NodesTasks#{WorkerNode => AssignedTask},
                  {AssignedTask, State#coor_state{tasks = NewTasks, nodes_tasks = NewNodesTasks}};
                none -> % all failed, assign a wait task
                  WaitTask = tasks:new_wait_task(),
                  {WaitTask,
                   State#coor_state{tasks = Tasks,
                                    nodes_tasks = NodesTasks#{WorkerNode => WaitTask}}}
              end;
            false ->
              WaitTask = tasks:new_wait_task(),
              {WaitTask,
               State#coor_state{tasks = Tasks, nodes_tasks = NodesTasks#{WorkerNode => WaitTask}}}
          end
      end
  end.

%%----------------------------------------------------------------------
%% @doc
%% Given a task and the server state, set the given task to be finished.
%% If all tasks are finished, then generate a new list of tasks and also
%% the stage number for the next stage.
%%
%% Note that this function can be called by slow workers, or workers that
%% were down and rejoined the network.
%%
%% @returns a list of new tasks, and an integer representing the stage number.
%%
%% @end
%%----------------------------------------------------------------------
-spec update_finished_task(mr_task(), {dl_db_instance(), dl_db_instance()}, state()) ->
                            state().
update_finished_task(Task = #task{assigned_worker = WorkerNode},
                     {WorkerFullDB, WorkerDeltaDB},
                     State =
                       #coor_state{tasks = Tasks,
                                   task_timeout = TimeOut,
                                   nodes_rate = NodesRate}) ->
  lager:debug("task_finished ~p", [Task]),
  case lists:splitwith(fun(T) -> not tasks:equals(T, Task) end, Tasks) of
    {_L, []} -> % the finished task is not in stage, ignore it
      % io:format("ignoring task with wrong stage ~p and current stage is ~p~n", [Task, SN]),
      State;
    {L1, [L2H | L2T]} ->
      TimeTaken = erlang:monotonic_time(millisecond) - tasks:get_start_time(L2H),
      L2H2 = tasks:set_finished(L2H),

      NewTimeOut = avg_timeout(TimeTaken, TimeOut),
      ProgRate = L2H#task.size / (TimeTaken + 2000),
      NewProgRate = exp_avg(ProgRate, maps:get(WorkerNode, NodesRate)),
      NewTasks = L1 ++ [L2H2 | L2T],

      {UpdatedFullPair, UpdatedDeltaPair} =
        update_state_dbs({WorkerFullDB, WorkerDeltaDB}, State),

      gen_new_state_when_task_done(NewTasks,
                                   State#coor_state{task_timeout = NewTimeOut,
                                                    nodes_rate =
                                                      NodesRate#{WorkerNode := NewProgRate},
                                                    full_pair = UpdatedFullPair,
                                                    delta_pair = UpdatedDeltaPair})
  end.

  -spec update_state_dbs({dl_db_instance(), dl_db_instance()}, state()) ->
                        {{dl_db_instance(), dl_db_instance()},
                         {dl_db_instance(), dl_db_instance()}}.
update_state_dbs({WorkerFullDB, WorkerDeltaDB},
                 #coor_state{full_pair = {CurFull, NextFull},
                             delta_pair = {CurDelta, NextDelta}}) ->
  FullPair = {CurFull, dbs:union(NextFull, WorkerFullDB)},
  DeltaPair = {CurDelta, dbs:union(NextDelta, WorkerDeltaDB)},
  {FullPair, DeltaPair}.

-spec programs_to_tasks(dl_db_instance(), [dl_program()], integer(), integer()) ->
                         [mr_task()].
programs_to_tasks(EDB, Programs, ProgNum, NumTasks) ->
  lists:flatmap(fun(Program) -> program_to_tasks(EDB, Program, ProgNum, NumTasks) end,
                Programs).

%% @doc Given a program, generate the initial delta and full db, as well as all the tasks
-spec program_to_tasks(dl_db_instance(), dl_program(), integer(), integer()) ->
                        [mr_task()].
program_to_tasks(EDB, Program, ProgNum, NumTasks) ->
  % the coordinator would do a first round of evaluation to find all deltas for
  % first stage of seminaive eval
  {_FullDB, DeltaDB} = program_to_dbs(EDB, Program),
  % similar to what I did in eval_seminaive, we put DeltaDB union EDB as the
  % DeltaDB in case the input contain "idb" predicates
  generate_one_stage_tasks(ProgNum, [Program], 1, NumTasks, DeltaDB).

% -spec programs_to_dbs(dl_db_instance(), [dl_program()]) ->
%                        {[{dl_db_instance(), dl_db_instance()}],
%                         [{dl_db_instance(), dl_db_instance()}]}.
% programs_to_dbs(EDB, Programs) ->
%   DBs = lists:map(fun(Program) -> program_to_dbs(EDB, Program) end, Programs),
%   {FullDBs, DeltaDBs} = lists:unzip(DBs),
%   {[{FullDB, dbs:new()} || FullDB <- FullDBs],
%    [{DeltaDB, dbs:new()} || DeltaDB <- DeltaDBs]}.

-spec program_to_dbs(dl_db_instance(), dl_program()) ->
                      {dl_db_instance(), dl_db_instance()}.
program_to_dbs(EDB, Program) ->
  EDBProg = eval:get_edb_program(Program),
  DeltaDB = eval:imm_conseq(EDBProg, EDB, dbs:new()),
  FullDB = dbs:union(DeltaDB, EDB),
  {FullDB, DeltaDB}.

%% @doc Given the current task, generate a new state with the new program.
-spec next_program_state(state()) -> state().
next_program_state(State =
                     #coor_state{prog_num = ProgNum,
                                 programs = Programs,
                                 full_pair = {CurFull, NextFull},
                                 num_tasks = NumTasks,
                                 nodes_rate = NodesRate,
                                 nodes_tasks = NodesTasks}) ->
  FinalDB = dbs:union(CurFull, NextFull),
  NewProgNum = ProgNum + 1,
  CurProgram = lists:nth(NewProgNum, Programs),
  Tasks = program_to_tasks(FinalDB, CurProgram, NewProgNum, NumTasks),
  {NewFullDB, NewDeltaDB} = program_to_dbs(FinalDB, CurProgram),
  State#coor_state{tasks = Tasks,
                   prog_num = NewProgNum,
                   full_pair = {NewFullDB, dbs:new()},
                   delta_pair = {NewDeltaDB, dbs:new()},
                   stage_num = 1,
                   task_timeout = ?INITIAL_TIMEOUT,
                   nodes_rate =
                     maps:from_keys(
                       maps:keys(NodesRate), ?INITIAL_RATE),
                   nodes_tasks =
                     maps:from_keys(
                       maps:keys(NodesTasks), tasks:new_wait_task())}.

next_stage_state(Ts,
                 State =
                   #coor_state{stage_num = SN,
                               full_pair = {CurFull, NextFull},
                               delta_pair = {CurDelta, NextDelta}}) ->
  lager:debug("new_tasks_for_round ~p tasks ~p", [SN + 1, Ts]),
  State#coor_state{tasks = Ts,
                   full_pair = {dbs:union(CurFull, NextFull), dbs:new()},
                   delta_pair = {dbs:union(CurDelta, NextDelta), dbs:new()},
                   stage_num = SN + 1}.

final_state(State =
              #coor_state{stage_num = SN,
                          tmp_path = TmpPath,
                          full_pair = {CurFull, NextFull}}) ->
  % nothing to generate, and we have evaluted all programs
  io:format("eval finished at stage ~p~n", [SN]),
  FinalDB = dbs:union(CurFull, NextFull),
  io:format("final db is ~n~s~n", [dbs:to_string(FinalDB)]),
  dbs:write_db(TmpPath ++ "final_db", FinalDB),
  send_done_msg(),
  State#coor_state{tasks = [tasks:new_terminate_task()], stage_num = SN + 1}.

%% @doc we generate a new state when a task has finished
-spec gen_new_state_when_task_done([mr_task()], state()) -> state().
gen_new_state_when_task_done(NewTasks,
                             State =
                               #coor_state{stage_num = SN,
                                           num_tasks = NumTasks,
                                           prog_num = ProgNum,
                                           programs = Programs,
                                           delta_pair = {_CurDelta, NextDelta}}) ->
  case check_all_finished(NewTasks) of
    true ->
      ?LOG_DEBUG(#{finished_stage => SN}),
      case generate_one_stage_tasks(ProgNum,
                                    [lists:nth(ProgNum, Programs)],
                                    SN + 1,
                                    NumTasks,
                                    NextDelta)
      of
        [] when ProgNum == length(Programs) ->
          final_state(State);
        [] -> % we should go to the next program and start again
          next_program_state(State);
        Ts -> % we enter the next round
          next_stage_state(Ts, State)
      end;
    false ->
      State#coor_state{tasks = NewTasks, stage_num = SN}
  end.

send_done_msg() ->
  case whereis(done_checker) of
    undefined ->
      ok;
    _Pid ->
      done_checker ! {self(), task_done}
  end.

collect_results(TaskNum, TmpPath, NumTasks) ->
  collect_results(get_prog_num(), TaskNum, TmpPath, NumTasks).

%%----------------------------------------------------------------------
%% @doc
%% Collect all results that have been generated in the full db.
%%
%% @returns the aggregated results
%%
%% @end
%%----------------------------------------------------------------------
-spec collect_results(integer(), integer(), file:filename(), integer()) ->
                       dl_db_instance().
collect_results(ProgNum, TaskNum, TmpPath, NumTasks) ->
  case TaskNum > NumTasks of
    true ->
      dbs:new();
    false ->
      FileName = io_lib:format("~s-~w-1-~w", [TmpPath ++ "fulldb", ProgNum, TaskNum]),
      DB = dbs:read_db(FileName),
      dbs:union(DB, collect_results(ProgNum, TaskNum + 1, TmpPath, NumTasks))
  end.

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
-spec generate_one_stage_tasks(integer(),
                               [dl_program()],
                               integer(),
                               integer(),
                               dl_db_instance()) ->
                                [mr_task()].
generate_one_stage_tasks(ProgNum, Programs, StageNum, NumTasks, NewDelta)
  when StageNum > 1 ->
  FragDelta = frag:hash_frag(NewDelta, lists:append(Programs), NumTasks),
  lists:flatmap(fun(Program) ->
                   lists:filtermap(fun(TaskNum) ->
                                      TaskDelta = lists:nth(TaskNum, FragDelta),
                                      case eval:is_fixpoint(TaskDelta) of
                                        true -> false; % if empty then do not generate anything
                                        false ->
                                          {true,
                                           tasks:new_task(Program,
                                                          ProgNum,
                                                          StageNum,
                                                          TaskNum,
                                                          TaskDelta)}
                                      end
                                   end,
                                   lists:seq(1, NumTasks))
                end,
                Programs);
generate_one_stage_tasks(ProgNum, Programs, StageNum, NumTasks, NewDelta)
  when StageNum == 1 ->
  FragDelta = frag:hash_frag(NewDelta, lists:append(Programs), NumTasks),
  lists:flatmap(fun(Program) ->
                   [tasks:new_task(Program,
                                   ProgNum,
                                   StageNum,
                                   TaskNum,
                                   lists:nth(TaskNum, FragDelta))
                    || TaskNum <- lists:seq(1, NumTasks)]
                end,
                Programs).

%%----------------------------------------------------------------------
%% @doc
%% This function will synchronously wait and return only if the master
%% has finished its job.
%% @end
%%----------------------------------------------------------------------
-spec wait_for_finish(timeout()) -> ok | timeout.
wait_for_finish(Timeout) ->
  register(done_checker, self()),
  Res = wait_for_finish_rec(Timeout),
  unregister(done_checker),
  Res.

wait_for_finish_rec(Timeout) ->
  receive
    {_Pid, task_done} ->
      ok;
    {PortId, {data, DataMsg}} ->
      lager:info("received data from ~p, msg is ~s", [PortId, DataMsg]),
      wait_for_finish_rec(Timeout);
    X ->
      exit(X)
  after Timeout ->
    exit(exe_timeout)
  end.

%% @doc this will find all the dead nodes and reset their tasks
-spec reset_dead_tasks([mr_task()]) -> [mr_task()].
reset_dead_tasks(Tasks) ->
  Dead =
    sets:from_list(
      lists:filter(fun(Node) -> net_adm:ping(Node) =/= pong end, nodes())),
  Alive = sets:from_list(nodes()),
  lists:map(fun(T = #task{assigned_worker = Worker}) ->
               case tasks:is_eval(T)
                    andalso tasks:is_assigned(T)
                    andalso tasks:is_in_progress(T)
                    andalso (sets:is_element(Worker, Dead)
                             orelse not sets:is_element(Worker, Alive))
               of
                 true ->
                   io:format(standard_error, "found dead task ~p~n", [T]),
                   tasks:reset_task(T);
                 false -> T
               end
            end,
            Tasks).
