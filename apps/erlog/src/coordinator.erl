-module(coordinator).

-include("../include/task_repr.hrl").
-include("../include/data_repr.hrl").
-include("../include/coor_params.hrl").

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, assign_task/1, finish_task/1, stop_coordinator/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).

-compile(export_all).

-endif.

-behaviour(gen_server).

%%% Client API
-spec start_link(string()) -> {ok, pid()}.
start_link(ProgName) ->
  net_kernel:start([?COOR_NAME, shortnames]),
  {ok, Pid} = gen_server:start_link(?MODULE, [ProgName], []),
  global:register_name(coor, Pid),
  {ok, Pid}.

%% Synchronous call

assign_task(Pid) ->
  gen_server:call(Pid, assign).

finish_task(Pid) ->
  gen_server:cast(Pid, finish).

stop_coordinator(Pid) ->
  gen_server:call(Pid, terminate).

%%% Server functions

% there is a couple of things we need to do here
% read in rules and
init([ProgName]) ->
  {ok, Stream} = file:open("apps/erlog/test/eval_SUITE_data/" ++ ProgName, [read]),
  {Facts, Rules} = preproc:lex_and_parse(Stream),
  file:close(Stream),
  % preprocess rules
  Program = preproc:process_rules(Rules),
  ?LOG_DEBUG(#{input_prog => utils:to_string(Program)}),
  ?LOG_DEBUG(#{input_data => Facts}),
  % create EDB from input relations
  EDB = db_ops:from_list(Facts),

  % the coordinator would do a first round of evaluation to find all static relations
  NewDB = eval:imm_conseq(Program, EDB),
  FullDB = db_ops:add_db_unique(NewDB, EDB),

  % this is the program that will be sent to workers
  NonStaticProg = lists:filter(fun(R) -> not eval:static_relation(R, Program) end, Program),

  hash_frag(FullDB, NonStaticProg, ?NUM_TASKS),
  Tasks =
    [#task{num = X,
           state = idle,
           type = evaluate}
     || X <- lists:seq(1, ?NUM_TASKS)],

  io:format("tasks ~p~n", [Tasks]),
  {ok, [Tasks]}.

handle_call(assign, _From, Tasks) ->

  {reply,
   #task{num = 1,
         state = idle,
         type = wait},
   Tasks};
handle_call(terminate, _From, Tasks) ->
  {stop, normal, ok, Tasks}.

handle_cast({finish}, Tasks) ->
  {noreply, Tasks}.

handle_info(Msg, Tasks) ->
  io:format("Unexpected message: ~p~n", [Msg]),
  {noreply, Tasks}.

terminate(normal, _Tasks) ->
  io:format("coordinator terminated~n"),
  ok.

%%% Private functions





-spec hash_and_write_one(dl_atom(), [integer()], integer(), file:io_device()) -> ok.
hash_and_write_one(Atom, Cols, TaskNum, Stream) ->
  ToHash = dl_repr:get_atom_args_by_index(Cols, Atom),
  Num = erlang:phash2(ToHash, ?NUM_TASKS) + 1,
  case Num == TaskNum of
    true ->
      io:format(Stream, "~s.~n", [utils:to_string(Atom)]);
    false ->
      ok
  end.

%%----------------------------------------------------------------------
%% @doc
%% Function: hash_and_write
%% Purpose: given a bunch of atoms and columns of their args to hash on,
%% this function would hash according to the given column and write
%% them onto disk accordingly
%% Args: Atoms is the bunch of atoms
%% Returns:
%% E.g. (
%%  reachable(a, b)
%%  reachable(b, c)
%%  reachable(c, c)
%%  reachable(c, d)
%% )
%% hashing on the second col, and this would give us something like
%% file1 reachable(a, b)
%% file2 reachable(b, c), reachable(c, c)
%% file3 reachable(c, d)
%%----------------------------------------------------------------------
-spec hash_and_write(dl_db_instance(), [integer()], integer(), file:io_device()) -> ok.
hash_and_write(Atoms, Cols, TaskNum, Stream) ->
  db_ops:foreach(fun(Atom) -> hash_and_write_one(Atom, Cols, TaskNum, Stream) end, Atoms).

%% for each rule, check its body to see whether there will be a join,
%% if so partition atoms with the same name as the rule's body in the db.
-spec part_by_rule(dl_db_instance(), dl_rule(), integer(), file:io_device()) ->
                    dl_db_instance().
part_by_rule(DB, Rule, TaskNum, Stream) ->
  case Rule of
    #dl_rule{body = [A1 = #dl_atom{}, A2 = #dl_atom{}]} ->
      {C1, C2} = eval:get_overlap_cols(A1#dl_atom.args, A2#dl_atom.args),
      {Atoms1, DBRest1} = db_ops:get_rel_by_pred_and_rest(A1#dl_atom.pred_sym, DB),
      {Atoms2, DBRest2} = db_ops:get_rel_by_pred_and_rest(A2#dl_atom.pred_sym, DBRest1),
      hash_and_write(Atoms1, C1, TaskNum, Stream),
      hash_and_write(Atoms2, C2, TaskNum, Stream),
      DBRest2;
    #dl_rule{body = [#dl_atom{}]} ->
      % TODO just put everything else into every file
      % is this necessary?
      DB
  end.

%%----------------------------------------------------------------------
%% @doc
%% Function: part_by_rules
%% Purpose: Given all rules, look at each of them in turn, and partition
%% the db instance according to each of them.
%%
%% @see part_by_rule/4

-spec part_by_rules(dl_db_instance(), [dl_rule()], integer(), file:io_device()) -> ok.
part_by_rules(DB, [], _CurNum, _Stream) ->
  case db_ops:is_empty(DB) of
    true ->
      ok;
    false ->
      io:format("non empty db, ~s~n", [db_ops:to_string(DB)])
  end,
  ok;
part_by_rules(DB, [RH | RT], CurNum, Stream) ->
  DBRest = part_by_rule(DB, RH, CurNum, Stream),
  part_by_rules(DBRest, RT, CurNum, Stream).

hash_frag_rec(_DB, _Rules, CurNum, TotNum) when CurNum > TotNum ->
  ok;
hash_frag_rec(DB, Rules, CurNum, TotNum) ->
  FileName = io_lib:format("~stask-~w", [?INTER_DIR, CurNum]),
  {ok, Stream} = file:open(FileName, [append]),
  part_by_rules(DB, Rules, CurNum, Stream),
  file:close(Stream),
  hash_frag_rec(DB, Rules, CurNum + 1, TotNum).

%%----------------------------------------------------------------------
%% @doc
%% Function: hash_frag
%% Purpose: partition the db and write them onto disk
%% Args:
%% Returns: actual number of tasks being generated
%%----------------------------------------------------------------------
-spec hash_frag(dl_db_instance(), dl_rule(), integer()) -> ok.
hash_frag(DB, Rules, Num) ->
  hash_frag_rec(DB, Rules, 1, Num).
