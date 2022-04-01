-module(worker).

-export([start_link/0, start/0, start/1, start_link/1, start_working/0, stop/0]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

-include("../include/task_repr.hrl").
-include("../include/data_repr.hrl").

-include_lib("kernel/include/logger.hrl").

-define(coor_node, 'coor@127.0.0.1').
-define(SLEEP_TIME, 300).

-behaviour(gen_server).

-type working_mode() :: success | failure | straggle.
-type worker_spec() :: success | {failure, float()} | {straggle, float()}.

-export_type([worker_spec/0]).

-record(worker_state,
        {coor_pid :: pid(),
         prog :: dl_program(),
         num_tasks :: integer(),
         task_num :: integer(),
         stage_num :: integer(),
         tmp_path :: file:filename(),
         mode :: working_mode()}).

name() ->
  worker.

%%% Client callbacks

start() ->
  start(success).

start_link() ->
  start_link(success).

start(Mode) ->
  gen_server:start({local, name()}, ?MODULE, [Mode], []).

start_link(Mode) ->
  gen_server:start_link({local, name()}, ?MODULE, [Mode], []).

start_working() ->
  gen_server:cast(name(), work).

stop() ->
  gen_server:call(name(), terminate).

%%% Server functions

init([Mode]) ->
  true = net_kernel:connect_node(?coor_node),
  global:sync(), % make sure that worker sees the registered name
  CoorPid = global:whereis_name(coor),
  erpc:cast(?coor_node, coordinator, reg_worker, [node()]),
  Prog = erpc:call(?coor_node, coordinator, get_prog, []),
  NumTasks = erpc:call(?coor_node, coordinator, get_num_tasks, []),
  TmpPath = erpc:call(?coor_node, coordinator, get_tmp_path, [], 5000),
  spawn(fun check_coor/0),
  {ok,
   #worker_state{coor_pid = CoorPid,
                 prog = Prog,
                 num_tasks = NumTasks,
                 tmp_path = TmpPath,
                 task_num = 0,
                 stage_num = 0,
                 mode = Mode}}.

handle_call(terminate, _From, State) ->
  {stop, normal, ok, State}.

handle_cast(work, State = #worker_state{mode = Mode}) ->
  case Mode of
    failure ->
      spawn_link(fun abnormal_worker:crasher/0);
    _Other ->
      ok
  end,
  % TODO better use a supervisor approach
  spawn(fun() -> work(State) end),
  {noreply, State}.

terminate(normal, _State) ->
  net_kernel:stop(),
  init:stop();
terminate(coor_down, State) ->
  io:format("coordinator is down, terminate as well~n"),
  terminate(normal, State).

%%% Private functions

work(State =
       #worker_state{prog = Prog,
                     num_tasks = NumTasks,
                     tmp_path = TmpPath,
                     mode = Mode}) ->
  try erpc:call(?coor_node, coordinator, assign_task, [node()], 5000) of
    T = #task{type = evaluate,
              task_num = TaskNum,
              stage_num = StageNum} ->
      ?LOG_INFO(#{worker_node => node(), worker_state => State}),
      ?LOG_INFO(#{worker_node => node(), task_num => TaskNum, stage_num => StageNum}),
      
      % HACK no need to distinguish different stages for the FullDB, just keep adding
      FName1 = io_lib:format("~sfulldb-~w-~w", [TmpPath, 1, TaskNum]),
      % TODO so here we are combining all full_dbs together, so when we generate
      % new atoms at each worker, we can avoid duplicates being written to the
      % deltas. The need for this is due to the fact that one atom that has been
      % written to the delta and indeed used in previous iterations are not necessarily
      % from that worker, but there is no way for the generating worker to know
      % that atom has already been generated.
      FullDBs =
        [dbs:read_db(
           io_lib:format("~s-~w-~w", [TmpPath ++ "fulldb", 1, X]))
         || X <- lists:seq(1, NumTasks)],
      DeltaDBs = [dbs:read_db(
           io_lib:format("~s-~w-~w", [TmpPath ++ "task", StageNum, X]))
         || X <- lists:seq(1, NumTasks)],
      case Mode of
        straggle ->
          abnormal_worker:straggler();
        _ ->
          ok
      end,
      % we need the old delta db because we might want the delta db generated from
      % other workers
      OldDeltaDB = lists:foldl(fun(DB, Acc) -> dbs:union(DB, Acc) end, dbs:new(), DeltaDBs),
      FullDB = lists:foldl(fun(DB, Acc) -> dbs:union(DB, Acc) end, dbs:new(), FullDBs),
      ?LOG_INFO(#{worker_node => node(), reading_fulldb_from_file_named => FName1}),
      FName2 = io_lib:format("~stask-~w-~w", [TmpPath, StageNum, TaskNum]),
      % we need to take the diff between this delta and the FullDB because our delta
      % might be generated by other workers
      DeltaDB = dbs:read_db(FName2),
      ?LOG_DEBUG(#{reading_deltas_from_file_named => FName2,
                   delta_db_read_size => dbs:size(DeltaDB)}),
      % use imm_conseq/3
      % need to store FullDB somewhere for later stages of evaluation
      % and this is not a static state, it changes every iteration
      % and is potentially huge, so this cost might be quite large
      {NewFullDB, NewDeltaDB} = eval:eval_seminaive_one(Prog, dbs:union(FullDB, OldDeltaDB), DeltaDB),
      ?LOG_DEBUG(#{new_db => dbs:to_string(NewDeltaDB)}),
      % hash the new DB locally and write to disk
      % with only tuples that have not been generated before
      frag:hash_frag(NewDeltaDB, Prog, StageNum + 1, NumTasks, TmpPath ++ "task"),
      frag:hash_frag(
        dbs:subtract(NewFullDB, FullDB), Prog, 1, NumTasks, TmpPath ++ "fulldb"),
      % call finish task on coordinator
      finish_task(T),
      ?LOG_INFO("~p rpc results for finish at stage ~p task ~p ~n", [node(), StageNum, TaskNum]),
      % request new tasks
      ?LOG_INFO("~p stage-~w task-~w finished, requesting new task~n",
                [node(), StageNum, TaskNum]),
      NewState = State#worker_state{task_num = TaskNum, stage_num = StageNum},
      ?LOG_INFO(#{worker_node => node(), new_state => NewState}),
      work(NewState);
    #task{type = wait} ->
      ?LOG_INFO("~p this is a wait task, sleeping for ~p sec~n", [node(), ?SLEEP_TIME / 1000]),
      ?LOG_INFO(#{worker_state_while_waiting => State}),
      timer:sleep(?SLEEP_TIME),
      work(State);
    #task{type = terminate} ->
      ?LOG_DEBUG("~p all done, time to relax~n", [node()]);
    Other ->
      ?LOG_INFO("~p some other stuff ~p~n", [node(), Other])
    catch 
      error:{erpc, timeout} -> % let's try again
        ?LOG_INFO("~p rpc timed out, trying again~n", [node()]),
        work(State);
      exit:{exception, Reason} -> % exception from the gen_server side
        ?LOG_INFO("~p gen_server call timed out with reason ~p, trying again~n", [node(), Reason]),
        work(State)
  end.

%%% Private functions

-spec finish_task(mr_task()) -> ok.
finish_task(Task) ->
  try erpc:call(?coor_node, coordinator, finish_task, [Task, node()], 5000) of
    ok -> ok
  catch
    error:{erpc, timeout} ->
      ?LOG_INFO("~p finish task timeout, trying again~n", [node()]),
      finish_task(Task)
  end.

% TODO is this worker's responsability or its supervisor's responsibility
% we need to terminate top down from the supervisor of this worker
% the better way is to ask a higher level supervisor to see that the coordinator
% has died and hence stop worker_sup
% but for now just do this
check_coor() ->
  ok = net_kernel:monitor_nodes(true),
  receive
    {nodedown, ?coor_node} ->
      stop()
  end.
