-module(coordinator).

-behaviour(gen_server).

-include("../include/task_repr.hrl").
-include("../include/data_repr.hrl").

-include_lib("kernel/include/logger.hrl").

-import(dl_repr, [get_rule_headname/1]).

-export([start_link/3, start_link/2, start_link/1, get_tmp_path/0, get_prog/0,
         get_num_tasks/0, get_current_stage_num/0, assign_task/1, finish_task/1, done/0, stop/0,
         reg_worker/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([collect_results/3, wait_for_finish/1]).

-record(coor_state,
        {tasks :: [mr_task()],
         num_tasks :: non_neg_integer(),
         prog :: dl_program(),
         stage_num :: integer(),
         tmp_path :: file:filename(),
         task_timeout :: erlang:timeout(),
         node_speed :: #{node() => timer:time()}}).

-type state() :: #coor_state{}.

-ifdef(TEST).

-compile(export_all).

-endif.

-define(INITIAL_TIMEOUT, 240 * 1000).
% higher alpha discounts older observations faster
-define(TIMEOUT_ALPHA, 0.5).

-spec avg_timeout(timer:time(), timer:time()) -> timer:time().
avg_timeout(_TimeTaken, infinity) ->
  infinity;
avg_timeout(TimeTaken, CurTimeOut) ->
  % adding some leeway to tolerate slightly slower worker(s)
  avg_time(TimeTaken, CurTimeOut) + 3 * 1000.

-spec avg_time(timer:time(), erlang:timeout()) -> erlang:timeout().
avg_time(TimeTaken, CurTime) ->
  trunc(?TIMEOUT_ALPHA * TimeTaken + (1 - ?TIMEOUT_ALPHA) * CurTime).

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

get_tmp_path() ->
  gen_server:call({global, name()}, tmp_path).

get_num_tasks() ->
  gen_server:call({global, name()}, num_tasks).

get_prog() ->
  gen_server:call({global, name()}, prog).

assign_task(WorkerNode) ->
  gen_server:call({global, name()}, {assign, WorkerNode}).

finish_task(Task) ->
  gen_server:cast({global, name()}, {finish, Task}).

stop() ->
  gen_server:call({global, name()}, terminate).

done() ->
  gen_server:call({global, name()}, finished).

%%% Server functions

-spec init([string()]) -> {ok, state()}.
init([ProgName, TmpPath, NumTasks]) ->
  % we check the freshness of tmp just in case
  clean_tmp(TmpPath),
  ok = file:make_dir(TmpPath),
  {Facts, Rules} = preproc:lex_and_parse(file, ProgName),
  % preprocess rules
  Program = preproc:process_rules(Rules),
  ?LOG_DEBUG(#{input_prog => dl_repr:program_to_string(Program)}),
  ?LOG_DEBUG(#{input_data => Facts}),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),

  % the coordinator would do a first round of evaluation to find all deltas for
  % first stage of seminaive eval
  EDBProg = eval:get_edb_program(Program),
  DeltaDB = eval:imm_conseq(EDBProg, EDB, dbs:new()),
  FullDB = dbs:union(DeltaDB, EDB),
  frag:hash_frag(FullDB, Program, 1, NumTasks, TmpPath ++ "fulldb"),
  % similar to what I did in eval_seminaive, we put DeltaDB union EDB as the
  % DeltaDB in case the input contain "idb" predicates
  frag:hash_frag(FullDB, Program, 1, NumTasks, TmpPath ++ "task"),
  Tasks = generate_one_stage_tasks(1, TmpPath, NumTasks),
  ?LOG_DEBUG(#{tasks_after_coor_initialisation => Tasks}),
  {ok,
   #coor_state{tasks = Tasks,
               num_tasks = NumTasks,
               prog = Program,
               stage_num = 1,
               tmp_path = TmpPath,
               task_timeout = ?INITIAL_TIMEOUT,
               node_speed = maps:new()}}.

handle_call(stage_num, _From, State = #coor_state{stage_num = StageNum}) ->
  {reply, StageNum, State};
handle_call(tmp_path, _From, State = #coor_state{tmp_path = TmpPath}) ->
  {reply, TmpPath, State};
handle_call(prog, _From, State = #coor_state{prog = Prog}) ->
  ?LOG_DEBUG(#{assigned_prog_to_worker => dl_repr:program_to_string(Prog)}),
  {reply, Prog, State};
handle_call(num_tasks, _From, State = #coor_state{num_tasks = NumTasks}) ->
  {reply, NumTasks, State};
handle_call({assign, WorkerNode}, _From, State = #coor_state{tasks = Tasks}) ->
  {Task, NewState} = find_next_task(State, WorkerNode),
  ?LOG_DEBUG(#{assigned_task_from_server => Task}),
  ?LOG_DEBUG(#{old_tasks => Tasks, new_tasks => NewState#coor_state.tasks}),
  {reply, Task, NewState};
handle_call(finished, _From, State = #coor_state{tasks = Tasks}) ->
  case Tasks of
    [#task{type = terminate}] ->
      {reply, true, State};
    _Ts ->
      {reply, false, State}
  end;
handle_call(terminate, _From, State) ->
  {stop, normal, ok, State}.

handle_cast({reg, WorkerNode}, State = #coor_state{node_speed = NodeSpeed}) ->
  NewNodeSpeed = NodeSpeed#{WorkerNode => ?INITIAL_TIMEOUT},
  io:format("registered ~p~n", [NewNodeSpeed]),
  {noreply, State#coor_state{node_speed = NewNodeSpeed}};
handle_cast({finish, Task}, State = #coor_state{}) ->
  NewState = update_finished_task(Task, State),
  % TODO can check NewTasks and see if we need to terminate the VM
  {noreply, NewState}.

handle_info(Msg, State) ->
  ?LOG_NOTICE("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(normal, #coor_state{tmp_path = TmpPath}) ->
  clean_tmp(TmpPath),
  ?LOG_DEBUG("coordinator terminated~n", []),
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

%%----------------------------------------------------------------------
%% @doc
%% A task is assignable if
%% <ol>
%%  <li> it is idle </li>
%%  <li> it is in the state in_progress and has timed out </li>
%% </ol>
%% @end
%%----------------------------------------------------------------------
-spec assignable(mr_task(), integer()) -> boolean().
assignable(#task{state = idle}, _TimeOut) ->
  true;
assignable(T = #task{state = in_progress, start_time = StartTime}, TimeOut) ->
  case erlang:monotonic_time(millisecond) - StartTime > TimeOut of
    true ->
      io:format("found time out task: ~p~n", [T]),
      true;
    false ->
      false
  end;
assignable(#task{}, _TimeOut) ->
  false.

-spec timed_out_task(mr_task(), timeout()) -> boolean().
timed_out_task(T = #task{state = in_progress, start_time = StartTime}, TimeOut) ->
  case erlang:monotonic_time(millisecond) - StartTime > TimeOut of
    true ->
      io:format("found time out task: ~p~n", [T]),
      true;
    false ->
      false
  end;
timed_out_task(_T,
               _TimeOut) -> % if the task is not in progress, then it has not timed out
  false.

-spec find_idle_task([mr_task()], node()) -> {mr_task(), [mr_task()]} | none.
find_idle_task(Tasks, WorkerNode) ->
  case lists:splitwith(fun(T) -> not tasks:is_idle(T) end, Tasks) of
    {NonIdle, [IdleH | IdleT]} ->
      NewTask = tasks:set_in_prog(IdleH),
      AssignedTask = tasks:set_worker(NewTask, WorkerNode),
      {AssignedTask, NonIdle ++ [AssignedTask | IdleT]};
    {_NonIdle, []} ->
      none
  end.

-spec find_timed_out_task([mr_task()], node(), timeout()) ->
                           {mr_task(), [mr_task()]} | none.
find_timed_out_task(Tasks, WorkerNode, TimeOut) ->
  case lists:splitwith(fun(T) -> not timed_out_task(T, TimeOut) end, Tasks) of
    {TimedIn, [TimedOutH | TimedOutT]} ->
      NewTask = tasks:reset_time(TimedOutH),
      AssignedTask = tasks:set_worker(NewTask, WorkerNode),
      {AssignedTask, TimedIn ++ [AssignedTask | TimedOutT]};
    {_TimedIn, []} ->
      none
  end.

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
-spec find_next_task(state(), node()) -> {mr_task(), state()}.
find_next_task(State =
                 #coor_state{tasks = Tasks1,
                             task_timeout = TimeOut,
                             node_speed = NodeSpeed},
               WorkerNode) ->
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
      case find_idle_task(Tasks, WorkerNode) % then we look for idle tasks
      of
        {AssignedTask, NewTasks} ->
          {AssignedTask, State#coor_state{tasks = NewTasks}};
        none ->
          case find_timed_out_task(Tasks, WorkerNode, TimeOut) % now we look for timed out tasks
          of
            {AssignedTask, NewTasks} ->
              {AssignedTask,
               State#coor_state{tasks = NewTasks, task_timeout = backoff_timeout(TimeOut)}};
            none ->
              case find_timed_out_task(Tasks,
                                       WorkerNode,
                                       maps:get(WorkerNode,
                                                NodeSpeed)) % finally go for backup tasks
              of
                {AssignedTask, NewTasks} ->
                  io:format("found a backup task assigned to worker ~p with higher speed "
                            "~p~n",
                            [WorkerNode, maps:get(WorkerNode, NodeSpeed)]),
                  {AssignedTask, State#coor_state{tasks = NewTasks}};
                none -> % all failed, assign a wait task
                  {tasks:new_wait_task(), State#coor_state{tasks = Tasks}}
              end
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
update_finished_task(Task = #task{assigned_worker = WorkerNode},
                     State =
                       #coor_state{tasks = Tasks,
                                   stage_num = SN,
                                   tmp_path = TmpPath,
                                   num_tasks = NumTasks,
                                   task_timeout = TimeOut,
                                   node_speed = NodeSpeed}) ->
  ?LOG_DEBUG(#{task_finished => Task}),
  case lists:splitwith(fun(T) -> not tasks:equals(T, Task) end, Tasks) of
    {_L, []} -> % the finished task is not in stage, ignore it
      State;
    {L1, [L2H | L2T]} ->
      L2H2 = tasks:set_finished(L2H),
      TimeTaken = erlang:monotonic_time(millisecond) - tasks:get_start_time(L2H),

      NewTimeOut = avg_timeout(TimeTaken, TimeOut),
      NewTime = avg_time(TimeTaken, maps:get(WorkerNode, NodeSpeed)),

      ?LOG_DEBUG("new time out is ~p~n", [NewTimeOut]),
      NewTasks = L1 ++ [L2H2 | L2T],
      case check_all_finished(NewTasks) of
        true ->
          ?LOG_DEBUG(#{finished_stage => SN}),
          case generate_one_stage_tasks(SN + 1, TmpPath, NumTasks) of
            [] -> % nothing to generate, we are done
              ?LOG_DEBUG(#{evaluation_finished_at_stage => SN}),
              io:format("eval finished at stage ~p~n", [SN]),
              FinalDB = collect_results(1, TmpPath, NumTasks),
              % io:format("final db is ~n~s~n", [dbs:to_string(FinalDB)]),
              dbs:write_db(TmpPath ++ "final_db", FinalDB),
              done_checker ! {self(), task_done},
              State#coor_state{tasks = [tasks:new_terminate_task()],
                               stage_num = SN + 1,
                               task_timeout = NewTimeOut,
                               node_speed = NodeSpeed#{WorkerNode := NewTime}};
            Ts -> % we enter the next round
              ?LOG_DEBUG(#{new_tasks_for_round => SN + 1, tasks => Ts}),
              State#coor_state{tasks = Ts,
                               stage_num = SN + 1,
                               task_timeout = NewTimeOut,
                               node_speed = NodeSpeed#{WorkerNode := NewTime}}
          end;
        false ->
          State#coor_state{tasks = NewTasks,
                           stage_num = SN,
                           task_timeout = NewTimeOut,
                           node_speed = NodeSpeed#{WorkerNode := NewTime}}
      end
  end.

%%----------------------------------------------------------------------
%% @doc
%% Collect all results that have been generated in the full db.
%%
%% @returns the aggregated results
%%
%% @end
%%----------------------------------------------------------------------
-spec collect_results(integer(), file:filename(), integer()) -> dl_db_instance().
collect_results(TaskNum, TmpPath, NumTasks) ->
  case TaskNum > NumTasks of
    true ->
      dbs:new();
    false ->
      FileName = io_lib:format("~s-1-~w", [TmpPath ++ "fulldb", TaskNum]),
      DB = dbs:read_db(FileName),
      dbs:union(DB, collect_results(TaskNum + 1, TmpPath, NumTasks))
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
-spec generate_one_stage_tasks(integer(), file:filename(), integer()) -> [mr_task()].
generate_one_stage_tasks(StageNum, TmpPath, NumTasks) when StageNum > 1 ->
  lists:filtermap(fun(TaskNum) ->
                     case check_fixpoint(StageNum, TaskNum, TmpPath) of
                       true -> false; % if empty then do not generate anything
                       false -> {true, tasks:new_task(StageNum, TaskNum)}
                     end
                  end,
                  lists:seq(1, NumTasks));
generate_one_stage_tasks(StageNum, _TmpPath, NumTasks) when StageNum == 1 ->
  [tasks:new_task(StageNum, TaskNum) || TaskNum <- lists:seq(1, NumTasks)].

%%----------------------------------------------------------------------
%% @doc
%% Checks whether we have reached a fixpoint by comparing the difference
%% between the result of this stage and the previous stage.
%% @end
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

spin_checker(Freq) ->
  case coordinator:done() of
    true ->
      io:format("The coordinator has finished its job.~n"),
      exit(done);
    false ->
      timer:sleep(Freq),
      % io:format("still waiting, currently the coordinator is in stage ~p of "
      %           "execution~n",
      %           [coordinator:get_current_stage_num()]),
      spin_checker(Freq)
  end.

%%----------------------------------------------------------------------
%% @doc
%% This function will synchronously wait and return only if the master
%% has finished its job.
%% @end
%%----------------------------------------------------------------------
-spec wait_for_finish(timeout()) -> ok | timeout.
wait_for_finish(Timeout) ->
  register(done_checker, self()),
  Res = receive
    {_Pid, task_done} ->
      ok;
    X ->
      io:format("received something else ~p~n", [X]),
      exit(X)
    after Timeout ->
      exit(exe_timeout)
    end,
  unregister(done_checker),
  Res.

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
