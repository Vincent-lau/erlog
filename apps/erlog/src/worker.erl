-module(worker).

-export([start_link/0, start/0, start/1, start_link/1, start_working/0,
         start_working_sync/0, stop/0]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

-include("../include/task_repr.hrl").

-define(coor_node, 'coor@127.0.0.1').
-define(SLEEP_TIME, 300).

-behaviour(gen_server).

-type working_mode() :: success | failure | straggle.
-type worker_spec() :: success | {failure, float()} | {straggle, float()}.

-export_type([worker_spec/0]).

-record(worker_state,
        {num_tasks :: integer(),
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

start_working_sync() ->
  {ok, NumTasks} = application:get_env(erlog, num_tasks),
  {ok, TmpPath} = application:get_env(erlog, inter_dir),
  work(#worker_state{num_tasks = NumTasks,
                     tmp_path = TmpPath,
                     task_num = 0,
                     stage_num = 0,
                     mode = success}).

stop() ->
  gen_server:call(name(), terminate).

%%% Server functions

init([Mode]) ->
  true = net_kernel:connect_node(?coor_node),
  global:sync(), % make sure that worker sees the registered name
  erpc:cast(?coor_node, coordinator, reg_worker, [node()]),
  NumTasks = call_coor(get_num_tasks, []),
  TmpPath = call_coor(get_tmp_path, []),
  spawn_link(fun check_coor/0),
  {ok,
   #worker_state{num_tasks = NumTasks,
                 tmp_path = TmpPath,
                 task_num = 0,
                 stage_num = 0,
                 mode = Mode}}.

handle_call(terminate, _From, State) ->
  {stop, normal, ok, State};
handle_call(work_sync, _From, State = #worker_state{mode = Mode}) ->
  case Mode of
    failure ->
      spawn_link(fun abnormal_worker:crasher/0);
    _Other ->
      ok
  end,
  work(State),
  {reply, ok, State}.

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
  lager:debug("coordinator is down, terminate as well~n"),
  terminate(normal, State).

%%% Private functions

work(State =
       #worker_state{num_tasks = NumTasks,
                     tmp_path = TmpPath,
                     mode = Mode}) ->
  case call_coor(assign_task, [node()]) of
    T = #task{type = evaluate,
              task_num = TaskNum,
              stage_num = StageNum,
              prog = Program,
              prog_num = ProgNum} ->
      % HACK no need to distinguish different stages for the FullDB, just keep adding
      FName1 = io_lib:format("~sfulldb-~w-~w-~w", [TmpPath, ProgNum, 1, TaskNum]),
      % here we are combining all full_dbs together, so when we generate
      % new atoms at each worker, we can avoid duplicates being written to the
      % deltas. The need for this is due to the fact that one atom that has been
      % written to the delta and indeed used in previous iterations are not necessarily
      % from that worker, but there is no way for the generating worker to know
      % that atom has already been generated.
      FullDBPath = io_lib:format("~s-~w", [TmpPath ++ "fulldb", ProgNum]),
      FullDB = dbs:read_db(FullDBPath),

      check_straggle(Mode),
      % we need the old delta db because we might want the delta db generated from
      % other workers
      lager:debug("worker_node ~p, reading_fulldb_from_file_named ~p", [node(), FName1]),
      FName2 = io_lib:format("~stask-~w-~w-~w", [TmpPath, ProgNum, StageNum, TaskNum]),
      % we need to take the diff between this delta and the FullDB because our delta
      % might be generated by other workers
      DeltaDB = dbs:read_db(FName2),
      lager:debug("reading_deltas_from_file_named ~p, delta_db_read_size ~p",
                  [FName2, dbs:size(DeltaDB)]),
      % use imm_conseq/3
      % need to store FullDB somewhere for later stages of evaluation
      % and this is not a static state, it changes every iteration
      % and is potentially huge, so this cost might be quite large
      {NewFullDB, NewDeltaDB} = eval:eval_seminaive_one(Program, FullDB, DeltaDB),
      lager:debug("new_db ~p", [dbs:to_string(NewDeltaDB)]),
      % hash the new DB locally and write to disk
      % with only tuples that have not been generated before
      frag:hash_frag(NewDeltaDB, Program, ProgNum, StageNum + 1, NumTasks, TmpPath ++ "task"),
      FullDBToWrite =
        dbs:subtract(
          dbs:union(NewFullDB, DeltaDB), FullDB),
      dbs:write_db(FullDBPath, FullDBToWrite),
      % call finish task on coordinator
      finish_task(T),
      lager:debug("~p rpc results for finish at stage ~p task ~p", [node(), StageNum, TaskNum]),
      % request new tasks
      lager:debug("~p stage-~w task-~w finished, requesting new task",
                  [node(), StageNum, TaskNum]),
      NewState = State#worker_state{task_num = TaskNum, stage_num = StageNum},
      lager:debug("worker_node ~p, new_state ~p", [node(), NewState]),
      work(NewState);
    #task{type = wait} ->
      lager:debug("~p this is a wait task, sleeping for ~p sec", [node(), ?SLEEP_TIME / 1000]),
      timer:sleep(?SLEEP_TIME),
      work(State);
    #task{type = terminate} ->
      lager:debug("~p all done, time to relax", [node()]);
    Other ->
      lager:info("~p some other stuff ~p~n", [node(), Other])
  end.

%%% Private functions

call_coor(Function, Args) ->
  call_coor(Function, Args, 5000).

call_coor(Function, Args, Timeout) ->
  try erpc:call(?coor_node, coordinator, Function, Args, 5000) of
    Reply ->
      lager:debug("~p function call ~p succeed", [node(), Function]),
      Reply
  catch
    error:{erpc, timeout} ->
      lager:debug("~p function ~p task timeout, trying again~n", [node(), Function]),
      timer:sleep(Timeout + rand:uniform(2000)),
      call_coor(Function, Args, Timeout * 2);
    exit:{exception, Reason} ->
      lager:debug("~p, gen_server call coordinator side exception ~p, trying again",
                  [node(), Reason]),
      timer:sleep(Timeout + rand:uniform(2000)),
      call_coor(Function, Args, Timeout * 2)
  end.

-spec finish_task(mr_task()) -> ok.
finish_task(Task) ->
  call_coor(finish_task, [Task, node()]).

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

check_straggle(straggle) ->
  abnormal_worker:straggler();
check_straggle(_Other) ->
  ok.
