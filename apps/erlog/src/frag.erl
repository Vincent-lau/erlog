-module(frag).

-compile(export_all).

-include("../include/data_repr.hrl").
-include_lib("kernel/include/logger.hrl").

-spec hash_and_write_one(dl_atom(),
                         [integer()],
                         integer(),
                         integer(),
                         file:io_device()) ->
                          ok.
hash_and_write_one(Atom, Cols, TaskNum, TotTasks, Stream) ->
  ToHash = dl_repr:get_atom_args_by_index(Cols, Atom),
  Num = erlang:phash2(ToHash, TotTasks) + 1,
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
%% @end
%%----------------------------------------------------------------------
-spec hash_and_write(dl_db_instance(),
                     [integer()],
                     integer(),
                     integer(),
                     file:io_device()) ->
                      ok.
hash_and_write(Atoms, Cols, TaskNum, TotTasks, Stream) ->
  dbs:foreach(fun(Atom) -> hash_and_write_one(Atom, Cols, TaskNum, TotTasks, Stream) end,
                 Atoms).

%% for each rule, check its body to see whether there will be a join,
%% if so partition atoms with the same name as the rule's body in the db.
-spec part_by_rule(dl_db_instance(), dl_rule(), integer(), integer(), file:io_device()) ->
                    dl_db_instance().
part_by_rule(DB, Rule, TaskNum, TotTasks, Stream) ->
  case Rule of
    #dl_rule{body = [A1 = #dl_atom{}, A2 = #dl_atom{}]} ->
      {C1, C2} = eval:get_overlap_cols(A1#dl_atom.args, A2#dl_atom.args),
      {Atoms1, DBRest1} = dbs:get_rel_by_pred_and_rest(A1#dl_atom.pred_sym, DB),
      {Atoms2, DBRest2} = dbs:get_rel_by_pred_and_rest(A2#dl_atom.pred_sym, DBRest1),
      hash_and_write(Atoms1, C1, TaskNum, TotTasks, Stream),
      hash_and_write(Atoms2, C2, TaskNum, TotTasks, Stream),
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

-spec part_by_rules(dl_db_instance(),
                    [dl_rule()],
                    integer(),
                    integer(),
                    file:io_device()) ->
                     ok.
part_by_rules(DB, [], _CurNum, _TotTasks, _Stream) ->
  case dbs:is_empty(DB) of
    true ->
      ok;
    false ->
      % TODO change this to LOG_NOTICE and add more handler
      io:format("non empty db after examining all rules, ~s~n", [dbs:to_string(DB)])
  end,
  ok;
part_by_rules(DB, [RH | RT], CurNum, TotTasks, Stream) ->
  DBRest = part_by_rule(DB, RH, CurNum, TotTasks, Stream),
  part_by_rules(DBRest, RT, CurNum, TotTasks, Stream).

-spec hash_frag_rec(dl_db_instance(),
                    [dl_rule()],
                    integer(),
                    integer(),
                    pos_integer(),
                    file:filename()) ->
                     ok.
hash_frag_rec(_DB, _Rules, CurNum, TotNum, _StageNum, _DirPath) when CurNum > TotNum ->
  ok;
hash_frag_rec(DB, Rules, CurNum, TotNum, StageNum, DirPath) ->
  % filename convention task-stage#-task#
  FileName = io_lib:format("~stask-~w-~w", [DirPath, StageNum, CurNum]),
  {ok, Stream} = file:open(FileName, [append]),
  part_by_rules(DB, Rules, CurNum, TotNum, Stream),
  file:close(Stream),
  hash_frag_rec(DB, Rules, CurNum + 1, TotNum, StageNum, DirPath).

%%----------------------------------------------------------------------
%% @doc
%% Function: hash_frag
%% Purpose: partition the db into Num parts and write them onto disk
%% Args:
%% Returns: actual number of tasks being generated
%% @end
%%----------------------------------------------------------------------
-spec hash_frag(dl_db_instance(), [dl_rule()], integer(), pos_integer(), file:filename()) -> ok.
hash_frag(DB, Rules, TotTaskNum, StageNum, DirPath) ->
  hash_frag_rec(DB, Rules, 1, TotTaskNum, StageNum, DirPath).
