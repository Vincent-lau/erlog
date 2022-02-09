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

-record(worker_state,
        {coor_pid :: pid(),
         prog :: dl_program(),
         num_tasks :: integer(),
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
  Prog = rpc:call(?coor_node, coordinator, get_prog, []),
  NumTasks = rpc:call(?coor_node, coordinator, get_num_tasks, []),
  spawn(fun check_coor/0),
  {ok,
   #worker_state{coor_pid = CoorPid,
                 prog = Prog,
                 num_tasks = NumTasks,
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
                     mode = Mode}) ->
  case erpc:call(?coor_node, coordinator, assign_task, []) of
    T = #task{type = evaluate,
              task_num = TaskNum,
              stage_num = StageNum} ->
      ?LOG_DEBUG(#{task_num => TaskNum, stage_num => StageNum}),
      TmpPath = erpc:call(?coor_node, coordinator, get_tmp_path, []),
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
      case Mode of
        straggle ->
          abnormal_worker:straggler();
        _ ->
          ok
      end,
      FullDB = lists:foldl(fun(DB, Acc) -> dbs:union(DB, Acc) end, dbs:new(), FullDBs),
      ?LOG_DEBUG(#{reading_fulldb_from_file_named => FName1,
                   full_db_read => dbs:to_string(FullDB)}),
      FName2 = io_lib:format("~stask-~w-~w", [TmpPath, StageNum, TaskNum]),
      % we need to take the diff between this delta and the FullDB because our delta
      % might be generated by other workers
      DeltaDB = dbs:read_db(FName2),
      ?LOG_DEBUG(#{reading_deltas_from_file_named => FName2,
                   delta_db_read => dbs:to_string(DeltaDB)}),
      % use imm_conseq/3
      % need to store FullDB somewhere for later stages of evaluation
      % and this is not a static state, it changes every iteration
      % and is potentially huge, so this cost might be quite large
      {NewFullDB, NewDeltaDB} = eval:eval_seminaive_one(Prog, FullDB, DeltaDB),
      ?LOG_DEBUG(#{new_db => dbs:to_string(NewDeltaDB)}),
      % hash the new DB locally and write to disk
      % with only tuples that have not been generated before
      frag:hash_frag(NewDeltaDB, Prog, StageNum + 1, NumTasks, TmpPath ++ "task"),
      frag:hash_frag(
        dbs:diff(NewFullDB, FullDB), Prog, 1, NumTasks, TmpPath ++ "fulldb"),
      % call finish task on coordinator
      erpc:cast(?coor_node, coordinator, finish_task, [T]),
      % request new tasks
      ?LOG_DEBUG("~p stage-~w task-~w finished, requesting new task~n",
                [node(), StageNum, TaskNum]),
      work(State#worker_state{mode = success});
    #task{type = wait} ->
      ?LOG_DEBUG("~p this is a wait task, sleeping for ~p sec~n", [node(), ?SLEEP_TIME / 1000]),
      timer:sleep(?SLEEP_TIME),
      work(State);
    #task{type = terminate} ->
      ?LOG_DEBUG("~p all done, time to relax~n", [node()]);
    {badrpc, Reason} -> % TODO do I need to distinguish different errors
      ?LOG_NOTICE("~p getting task from coordinator failed due to ~p~n", [node(), Reason])
  end.

%%% Private functions

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
