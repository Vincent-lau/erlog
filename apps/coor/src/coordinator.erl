-module(coordinator).

-behaviour(gen_server).

-include("../../dl_lib/include/task_repr.hrl").

-include_lib("kernel/include/logger.hrl").

-import(dl_repr, [get_rule_headname/1]).

-export([start_link/4, start_link/3, start_link/2, get_tmp_path/0, get_num_tasks/0,
         get_current_stage_num/0, get_prog_num/0, assign_task/1, finish_task/2, done/0, stop/0,
         reg_worker/1, dereg_worker/1, start_working/0, reset_worker/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([collect_results/2, wait_for_finish/1]).

-record(coor_state,
        {tasks :: [mr_task()],
         task_sup :: pid(),
         num_tasks :: integer(),
         qry_names :: [string()],
         programs :: [[dl_program()]],
         prog_num :: integer(),
         stage_num :: integer(),
         tmp_path :: file:filename(),
         task_timeout :: timeout(),
         worker_rate :: #{pid() => number()},
         worker_tasks :: #{pid() => mr_task()}}).

        % use task/file size divide execution time, exp avg to represent speed
        % paper uses ProgressScore/T to represent ProgressRate, here we do something
        % similar, just replacing ProgressScore with file size

-type state() :: #coor_state{}.

-ifdef(TEST).

-compile(export_all).

-endif.

-define(SPEC,
        #{id => task_coor_sup,
          start => {task_coor_sup, start_link, []},
          restart => temporary,
          shutdown => 10000,
          type => supervisor,
          modules => [task_coor_sup]}).
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

name() ->
  coor.

%%% Client API
-spec start_link(pid(), file:filename()) -> {ok, pid()}.
start_link(Sup, ProgName) ->
  {ok, TmpPath} = application:get_env(coor, inter_dir),
  start_link(Sup, ProgName, TmpPath).

-spec start_link(pid(), file:filename(), file:filename()) -> {ok, pid()}.
start_link(Sup, ProgName, TmpPath) ->
  {ok, NumTasks} = application:get_env(coor, num_tasks),
  start_link(Sup, ProgName, TmpPath, NumTasks).

-spec start_link(pid(), file:filename(), file:filename(), integer()) -> {ok, pid()}.
start_link(Sup, ProgName, TmpPath, NumTasks) ->
  gen_server:start_link({global, name()}, ?MODULE, [Sup, ProgName, TmpPath, NumTasks], []).

%% Synchronous call

worker_nodes() ->
  lists:filter(fun(Node) ->
                  case string:slice(hd(string:tokens(atom_to_list(Node), "@")), 0, 4) of
                    "work" -> true;
                    _Other -> false
                  end
               end,
               nodes()).

start_working() ->
  gen_server:cast({global, name()}, compute).

reg_worker(WorkerPid) ->
  gen_server:call({global, name()}, {reg, WorkerPid}).

dereg_worker(WorkerPid) ->
  gen_server:call({global, name()}, {dereg, WorkerPid}).

get_current_stage_num() ->
  gen_server:call({global, name()}, stage_num).

get_prog_num() ->
  gen_server:call({global, name()}, prog_num).

get_tmp_path() ->
  gen_server:call({global, name()}, tmp_path).

get_num_tasks() ->
  gen_server:call({global, name()}, num_tasks).

assign_task(WorkerPid) ->
  gen_server:call({global, name()}, {assign, WorkerPid}).

finish_task(Task, WorkerPid) ->
  gen_server:call({global, name()}, {finish, Task, WorkerPid}).

reset_worker(WorkerPid) ->
  gen_server:call({global, name()}, {reset, WorkerPid}).

stop() ->
  gen_server:call({global, name()}, terminate).

done() ->
  gen_server:call({global, name()}, finished).

%%% Server functions

-spec init([string()]) -> {ok, state()}.
init([Sup, ProgName, TmpPath, NumTasks]) ->
  % we check the freshness of tmp just in case
  clean_tmp(TmpPath),
  ok = file:make_dir(TmpPath),

  ets:new(dl_atom_names, [named_table, public]),
  lager:info("number of tasks is ~p", [NumTasks]),
  {Facts, Rules} = preproc:lex_and_parse(file, ProgName),
  % preprocess rules
  QryNames = preproc:get_output_name(ProgName),
  Program = preproc:process_rules(Rules),
  Programs = neg:compute_stratification(allorder, Program),
  ?LOG_DEBUG(#{input_prog => dl_repr:program_to_string(Program)}),
  ?LOG_DEBUG(#{input_data => Facts}),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),

  self() ! {start_task_coor_supervisor, Sup},
  self() ! {generate_tasks, {EDB, Programs, NumTasks, TmpPath}},

  {ok,
   #coor_state{num_tasks = NumTasks,
               qry_names = QryNames,
               programs = Programs,
               prog_num = 1,
               stage_num = 1,
               tmp_path = TmpPath,
               task_timeout = ?INITIAL_TIMEOUT,
               worker_rate = maps:new(),
               worker_tasks = maps:new()}}.

handle_call({reg, WorkerPid},
            _From,
            State = #coor_state{worker_rate = WorkerRate, worker_tasks = WorkerTasks}) ->
  NewWorkerRate = WorkerRate#{WorkerPid => ?INITIAL_RATE}, % to avoid division by zero
  NewWorkerTasks = WorkerTasks#{WorkerPid => tasks:new_wait_task()},
  {reply, ok, State#coor_state{worker_rate = NewWorkerRate, worker_tasks = NewWorkerTasks}};
handle_call({dereg, WorkerPid},
            _From,
            State = #coor_state{worker_rate = WorkerRate, worker_tasks = WorkerTasks}) ->
  NewWorkerRate = maps:remove(WorkerPid, WorkerRate),
  NewWorkerTasks = maps:remove(WorkerPid, WorkerTasks),
  {reply, ok, State#coor_state{worker_rate = NewWorkerRate, worker_tasks = NewWorkerTasks}};
handle_call(stage_num, _From, State = #coor_state{stage_num = StageNum}) ->
  {reply, StageNum, State};
handle_call(tmp_path, _From, State = #coor_state{tmp_path = TmpPath}) ->
  {reply, TmpPath, State};
handle_call(prog_num, _From, State = #coor_state{prog_num = ProgNum}) ->
  {reply, ProgNum, State};
handle_call(num_tasks, _From, State = #coor_state{num_tasks = NumTasks}) ->
  {reply, NumTasks, State};
handle_call({assign, WorkerPid},
            _From,
            State = #coor_state{tasks = Tasks, worker_tasks = WorkerTasks}) ->
  case tasks:is_eval(
         maps:get(WorkerPid, WorkerTasks))
  of
    true -> % if assigned an eval task, then give it a wait one
      {reply, maps:get(WorkerPid, WorkerTasks), State};
    false ->
      {Task, NewState} = find_next_task(State, WorkerPid),
      ?LOG_DEBUG(#{assigned_task_from_server => Task, to => WorkerPid}),
      ?LOG_DEBUG(#{old_tasks => Tasks, new_tasks => NewState#coor_state.tasks}),
      {reply, Task, NewState#coor_state{worker_tasks = WorkerTasks#{WorkerPid => Task}}}
  end;
handle_call({reset, WorkerPid}, _From, State = #coor_state{worker_tasks = WorkerTasks}) ->
  {reply, ok, State#coor_state{worker_tasks = maps:put(WorkerPid, none, WorkerTasks)}};
handle_call({finish, Task, WorkerPid},
            _From,
            State = #coor_state{worker_tasks = WorkerTasks}) ->
  NewState = update_finished_task(Task, State),
  {reply,
   ok,
   NewState#coor_state{worker_tasks = WorkerTasks#{WorkerPid => tasks:new_wait_task()}}};
handle_call(finished, _From, State = #coor_state{tasks = Tasks}) ->
  case Tasks of
    [#task{type = terminate}] ->
      {reply, true, State};
    _Ts ->
      {reply, false, State}
  end;
handle_call(terminate, _From, State) ->
  {stop, normal, ok, State}.

handle_cast({reg, WorkerPid},
            State = #coor_state{worker_rate = WorkerRate, worker_tasks = WorkerTasks}) ->
  NewWorkerRate = WorkerRate#{WorkerPid => ?INITIAL_RATE}, % to avoid division by zero
  NewWorkerTasks = WorkerTasks#{WorkerPid => tasks:new_wait_task()},
  {noreply, State#coor_state{worker_rate = NewWorkerRate, worker_tasks = NewWorkerTasks}};
handle_cast(compute, State) ->
  R1 = erpc:multicall(worker_nodes(), worker, start_working, []),
  lager:info("worker start_working rpc results ~p~n", [R1]),
  spawn(fun() ->
           Time = timer:tc(coordinator, wait_for_finish, [1000 * 60]),
           lager:info("time is ~p~n", [Time])
        end),
  {noreply, State}.

handle_info({start_task_coor_supervisor, Sup}, S = #coor_state{}) ->
  {ok, Pid} = supervisor:start_child(Sup, ?SPEC),
  true = register(task_coor_sup, Pid),
  link(Pid),
  {noreply, S#coor_state{task_sup = Pid}};
handle_info({generate_tasks, {EDB, Programs, NumTasks, TmpPath}},
            S = #coor_state{task_sup = TaskSup})
  when TaskSup =/= undefined ->
  Tasks = programs_to_tasks(EDB, hd(Programs), 1, NumTasks, TmpPath, TaskSup),
  {noreply, S#coor_state{tasks = Tasks}};
handle_info(Msg, State) ->
  lager:notice("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(normal, #coor_state{tmp_path = TmpPath}) ->
  ets:delete(dl_atom_names),
  clean_tmp(TmpPath),
  lager:debug("coordinator terminated~n", []),
  ok;
terminate(Err, S = #coor_state{}) ->
  lager:notice("error is ~p state is ~p~n", [Err, S]).

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

%% @doc this function will find an idle task, and if found, set its state appropriately
-spec find_set_idle_task([mr_task()], pid()) -> {mr_task(), [mr_task()]} | none.
find_set_idle_task(Tasks, WorkerPid) ->
  case lists:splitwith(fun(T) -> not tasks:is_idle(T) end, Tasks) of
    {NonIdle, [IdleH | IdleT]} ->
      ok = task_coor:request_task(IdleH#task.statem, WorkerPid),
      AssignedTask = tasks:set_worker(IdleH, WorkerPid),
      {AssignedTask, NonIdle ++ [AssignedTask | IdleT]};
    {_NonIdle, []} ->
      none
  end.

-spec possibly_faster(mr_task(), number(), number()) -> boolean().
possibly_faster(T = #task{size = Size, start_time = StartTime},
                AssignedWorkerRate,
                NewWorkerRate) ->
  case tasks:is_in_progress(T) of
    true ->
      EstCompletion = Size / NewWorkerRate,
      EstRemaining =
        Size / AssignedWorkerRate - (erlang:monotonic_time(millisecond) - StartTime),
      case EstRemaining < 0 of
        true ->
          true;
        false ->
          EstCompletion < EstRemaining
      end;
    false ->
      false
  end;
possibly_faster(_T, _A, _N) ->
  false.

-spec find_set_speculative_task([mr_task()], node(), #{node() => number()}) ->
                                 {mr_task(), [mr_task()]} | none.
find_set_speculative_task(Tasks, WorkerPid, WorkerRate) ->
  % for each task, calculate the estimated time, compare with estimated remaining time
  case lists:splitwith(fun(T) ->
                          not
                            possibly_faster(T,
                                            maps:get(T#task.assigned_worker, WorkerRate),
                                            maps:get(WorkerPid, WorkerRate))
                       end,
                       Tasks)
  of
    {DefSlower, [PosFasterH | PosFasterT]} ->
      NewTask = tasks:reset_time(PosFasterH),
      AssignedTask = tasks:set_worker(NewTask, WorkerPid),
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
%%    <li>the worker has timed out </li>
%%  </ol>
%% if there is none available, then give a wait task
%% @param WorkerPid is the node that is requesting the task
%% @returns the task that can be assigned, and the updated list of tasks.
%% @end
%%----------------------------------------------------------------------
-spec find_next_task(state(), node()) -> {mr_task(), state()}.
find_next_task(State =
                 #coor_state{tasks = Tasks,
                             worker_rate = WorkerRate,
                             worker_tasks = WorkerTasks},
               WorkerPid) ->
  case Tasks of
    [T = #task{type = terminate}] -> % only terminate task exists, assign it
      {T, State#coor_state{tasks = Tasks}};
    _Other ->
      case find_set_idle_task(Tasks, WorkerPid) % then we look for idle tasks
      of
        {AssignedTask, NewTasks} ->
          NewWorkerTasks = WorkerTasks#{WorkerPid => AssignedTask},
          {AssignedTask, State#coor_state{tasks = NewTasks, worker_tasks = NewWorkerTasks}};
        none ->
          case near_completion(Tasks) of
            true ->
              case find_set_speculative_task(Tasks,
                                             WorkerPid,
                                             WorkerRate) % finally go for backup tasks
              of
                {AssignedTask, NewTasks} ->
                  lager:debug("reassigning_task ~p, from ~p to ~p",
                              [AssignedTask, AssignedTask#task.assigned_worker, WorkerPid]),
                  NewWorkerTasks = WorkerTasks#{WorkerPid => AssignedTask},
                  {AssignedTask, State#coor_state{tasks = NewTasks, worker_tasks = NewWorkerTasks}};
                none -> % all failed, assign a wait task
                  WaitTask = tasks:new_wait_task(),
                  {WaitTask,
                   State#coor_state{tasks = Tasks,
                                    worker_tasks = WorkerTasks#{WorkerPid => WaitTask}}}
              end;
            false ->
              WaitTask = tasks:new_wait_task(),
              {WaitTask,
               State#coor_state{tasks = Tasks, worker_tasks = WorkerTasks#{WorkerPid => WaitTask}}}
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
-spec update_finished_task(mr_task(), state()) -> state().
update_finished_task(Task = #task{assigned_worker = WorkerPid},
                     State =
                       #coor_state{tasks = Tasks,
                                   task_timeout = TimeOut,
                                   worker_rate = WorkerRate}) ->
  lager:debug("task_finished ~p~n", [Task]),
  case lists:splitwith(fun(T) -> not tasks:equals(T, Task) end, Tasks) of
    {_L, []} -> % the finished task is not in stage, ignore it
      State;
    {_L1, [L2H | _L2T]} ->
      TimeTaken = erlang:monotonic_time(millisecond) - tasks:get_start_time(L2H),
      ok = tasks:set_finished(L2H),

      NewTimeOut = avg_timeout(TimeTaken, TimeOut),
      ProgRate = L2H#task.size / (TimeTaken + 2000),
      NewProgRate = exp_avg(ProgRate, maps:get(WorkerPid, WorkerRate)),
      gen_new_state_when_task_done(Tasks, NewProgRate, NewTimeOut, WorkerPid, State)
  end.

-spec programs_to_tasks(dl_db_instance(),
                        [dl_program()],
                        integer(),
                        integer(),
                        file:filename(),
                        pid()) ->
                         [mr_task()].
programs_to_tasks(EDB, Programs, ProgNum, NumTasks, TmpPath, Sup) ->
  lists:flatmap(fun(Program) ->
                   program_to_tasks(EDB, Program, ProgNum, NumTasks, TmpPath, Sup)
                end,
                Programs).

%% @doc Given a program, hash frag it to disk and generate corresponding tasks.
-spec program_to_tasks(dl_db_instance(),
                       dl_program(),
                       integer(),
                       integer(),
                       file:filename(),
                       pid()) ->
                        [mr_task()].
program_to_tasks(EDB, Program, ProgNum, NumTasks, TmpPath, TaskSup) ->
  % the coordinator would do a first round of evaluation to find all deltas for
  % first stage of seminaive eval
  EDBProg = eval:get_edb_program(Program),
  DeltaDB = eval:imm_conseq(EDBProg, EDB, dbs:new()),
  FullDB = dbs:union(DeltaDB, EDB),
  FullDBPath = io_lib:format("~s-~w", [TmpPath ++ "fulldb", ProgNum]),
  dbs:write_db(FullDBPath, FullDB),
  % similar to what I did in eval_seminaive, we put DeltaDB union EDB as the
  % DeltaDB in case the input contain "idb" predicates
  frag:hash_frag(FullDB, Program, ProgNum, 1, NumTasks, TmpPath ++ "task"),
  Tasks = generate_one_stage_tasks(ProgNum, [Program], 1, TmpPath, NumTasks, TaskSup),
  lager:debug("tasks_after_coor_initialisation ~p", [Tasks]),
  Tasks.

%% @doc Given the current task, generate a new state with the new program.
-spec new_program_state(state()) -> state().
new_program_state(State =
                    #coor_state{prog_num = ProgNum,
                                task_sup = TaskSup,
                                programs = Programs,
                                num_tasks = NumTasks,
                                worker_rate = WorkerRate,
                                worker_tasks = WorkerTasks,
                                tmp_path = TmpPath}) ->
  FinalDB = collect_results(TmpPath, ProgNum),
  NewProgNum = ProgNum + 1,
  Tasks =
    programs_to_tasks(FinalDB,
                      lists:nth(NewProgNum, Programs),
                      NewProgNum,
                      NumTasks,
                      TmpPath,
                      TaskSup),
  State#coor_state{tasks = Tasks,
                   prog_num = NewProgNum,
                   stage_num = 1,
                   task_timeout = ?INITIAL_TIMEOUT,
                   worker_rate =
                     maps:from_keys(
                       maps:keys(WorkerRate), ?INITIAL_RATE),
                   worker_tasks =
                     maps:from_keys(
                       maps:keys(WorkerTasks), tasks:new_wait_task())}.

%% @doc we generate a new state when a task has finished
-spec gen_new_state_when_task_done([mr_task()], number(), timeout(), node(), state()) ->
                                    state().
gen_new_state_when_task_done(NewTasks,
                             NewProgRate,
                             NewTimeOut,
                             WorkerPid,
                             State =
                               #coor_state{stage_num = SN,
                                           task_sup = TaskSup,
                                           tmp_path = TmpPath,
                                           num_tasks = NumTasks,
                                           prog_num = ProgNum,
                                           qry_names = QryNames,
                                           programs = Programs,
                                           worker_rate = WorkerRate}) ->
  case check_all_finished(NewTasks) of
    true ->
      ok = tasks:stop_all_statem(TaskSup),
      case generate_one_stage_tasks(ProgNum,
                                    lists:nth(ProgNum, Programs),
                                    SN + 1,
                                    TmpPath,
                                    NumTasks,
                                    TaskSup)
      of
        [] when ProgNum == length(Programs) ->
          % nothing to generate, and we have evaluted all programs
          lager:info("eval finished at stage ~p", [SN]),
          FinalDB = collect_results(TmpPath, ProgNum),
          QryRes = extract_results(FinalDB, QryNames),
          dbs:write_db(TmpPath ++ "final_db", QryRes),
          send_done_msg(),
          State#coor_state{tasks = [tasks:new_terminate_task()],
                           stage_num = SN + 1,
                           task_timeout = NewTimeOut,
                           worker_rate = WorkerRate#{WorkerPid := NewProgRate}};
        [] -> % we should go to the next program and start again
          new_program_state(State);
        Ts -> % we enter the next round
          lager:debug("new tasks for stage ~p", [SN + 1]),
          State#coor_state{tasks = Ts,
                           stage_num = SN + 1,
                           task_timeout = NewTimeOut,
                           worker_rate = WorkerRate#{WorkerPid := NewProgRate}}
      end;
    false ->
      State#coor_state{tasks = NewTasks,
                       stage_num = SN,
                       task_timeout = NewTimeOut,
                       worker_rate = WorkerRate#{WorkerPid := NewProgRate}}
  end.

-spec extract_results(dl_db_instance(), [string()]) -> dl_db_instance().
extract_results(DB, QryNames) ->
  ResQL =
    lists:foldl(fun(QryName, AccIn) ->
                   TmpDB = dbs:get_rel_by_name(QryName, DB),
                   dbs:union(TmpDB, AccIn)
                end,
                dbs:new(),
                QryNames),
  case dbs:size(ResQL) > 30 of
    false ->
      lager:info("final db is ~n~s", [dbs:to_string(ResQL)]);
    true ->
      lager:debug("final db is ~n~s", [dbs:to_string(ResQL)])
  end,
  ResQL.

send_done_msg() ->
  case whereis(done_checker) of
    undefined ->
      ok;
    _Pid ->
      done_checker ! {self(), task_done}
  end.

%% @doc Collect all results that have been generated in the full db.
collect_results(TmpPath, ProgNum) ->
  FileName = io_lib:format("~s-~w", [TmpPath ++ "fulldb", ProgNum]),
  dbs:read_db(FileName).

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
                               file:filename(),
                               integer(),
                               pid()) ->
                                [mr_task()].
generate_one_stage_tasks(ProgNum, Programs, StageNum, TmpPath, NumTasks, Sup)
  when StageNum > 1 ->
  lists:flatmap(fun(Program) ->
                   lists:filtermap(fun(TaskNum) ->
                                      case check_fixpoint(ProgNum, StageNum, TaskNum, TmpPath) of
                                        true -> false; % if empty then do not generate anything
                                        false ->
                                          {true,
                                           tasks:new_task(Program,
                                                          ProgNum,
                                                          StageNum,
                                                          TaskNum,
                                                          Sup,
                                                          TmpPath)}
                                      end
                                   end,
                                   lists:seq(1, NumTasks))
                end,
                Programs);
generate_one_stage_tasks(ProgNum, Programs, StageNum, TmpPath, NumTasks, Sup)
  when StageNum == 1 ->
  lists:flatmap(fun(Program) ->
                   [tasks:new_task(Program, ProgNum, StageNum, TaskNum, Sup, TmpPath)
                    || TaskNum <- lists:seq(1, NumTasks)]
                end,
                Programs).

%%----------------------------------------------------------------------
%% @doc
%% Checks whether we have reached a fixpoint by comparing the difference
%% between the result of this stage and the previous stage.
%% @end
%%----------------------------------------------------------------------
-spec check_fixpoint(integer(), integer(), integer(), file:filename()) -> boolean().
check_fixpoint(ProgNum, StageNum, TaskNum, TmpPath) ->
  FName1 =
    io_lib:format("~s-~w-~w-~w", [TmpPath ++ "task", ProgNum, StageNum - 1, TaskNum]),
  FName2 = io_lib:format("~s-~w-~w-~w", [TmpPath ++ "task", ProgNum, StageNum, TaskNum]),
  DB1 = dbs:read_db(FName1),
  DB2 = dbs:read_db(FName2),
  ?LOG_DEBUG(#{stage => StageNum,
               task_num => TaskNum,
               db1 => dbs:to_string(DB1),
               db2 => dbs:to_string(DB2),
               db_equal => dbs:equal(DB1, DB2)}),
  dbs:equal(DB1, DB2).

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
      lager:debug("received data from ~p, msg is ~s", [PortId, DataMsg]),
      wait_for_finish_rec(Timeout);
    X ->
      exit(X)
  after Timeout ->
    exit(exe_timeout)
  end.
