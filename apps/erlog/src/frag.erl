-module(frag).

-include("../include/data_repr.hrl").

-ifdef(TEST).

-compile(export_all).

-endif.

-export([hash_frag/3]).

-import(dl_repr, [get_atom_name/1]).

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
      io:format(Stream, "~s.~n", [dl_repr:atom_to_string(Atom)]);
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
  case dl_repr:get_rule_body_atoms(Rule) of
    [A1 = #dl_atom{}, A2 = #dl_atom{}] ->
      {C1, C2} = eval:get_overlap_cols(A1#dl_atom.args, A2#dl_atom.args),
      {Atoms1, DBRest1} = dbs:get_rel_by_name_and_rest(get_atom_name(A1), DB),
      {Atoms2, DBRest2} = dbs:get_rel_by_name_and_rest(get_atom_name(A2), DBRest1),
      hash_and_write(Atoms1, C1, TaskNum, TotTasks, Stream),
      hash_and_write(Atoms2, C2, TaskNum, TotTasks, Stream),
      DBRest2;
    _Other -> % this is not a join rule, so partition in the end
      DB
  end.

%%----------------------------------------------------------------------
%% @doc
%% Function: part_by_rules
%% Purpose: Given all rules, look at each of them in turn, and partition
%% the db instance according to each of them.
%%
%% @see part_by_rule/4
%% @end
%%----------------------------------------------------------------------

-spec part_by_rules(dl_db_instance(),
                    [dl_rule()],
                    integer(),
                    integer(),
                    file:io_device()) ->
                     ok.
part_by_rules(DB, [], CurTaskNum, TotTasks, Stream) ->
  % after examining all join rules, we can partition the rest of the db however
  % we like
  hash_and_write(DB, [1], CurTaskNum, TotTasks, Stream);
part_by_rules(DB, [RH | RT], CurTaskNum, TotTasks, Stream) ->
  DBRest = part_by_rule(DB, RH, CurTaskNum, TotTasks, Stream),
  part_by_rules(DBRest, RT, CurTaskNum, TotTasks, Stream).

-spec hash_frag_rec(dl_db_instance(),
                    [dl_rule()],
                    integer(),
                    integer(),
                    integer(),
                    pos_integer(),
                    file:filename()) ->
                     ok.
hash_frag_rec(_DB, _Rules, _ProgNum, _StageNum, CurTaskNum, TotNum, _DirPath)
  when CurTaskNum > TotNum ->
  ok;
hash_frag_rec(DB, Rules, ProgNum, StageNum, CurTaskNum, TotNum, DirPath) ->
  % filename convention task-prog#-stage#-task#
  FileName = io_lib:format("~s-~w-~w-~w", [DirPath, ProgNum, StageNum, CurTaskNum]),
  {ok, Stream} = file:open(FileName, [append]),
  part_by_rules(DB, Rules, CurTaskNum, TotNum, Stream),
  file:close(Stream),
  hash_frag_rec(DB, Rules, ProgNum, StageNum, CurTaskNum + 1, TotNum, DirPath).

%%----------------------------------------------------------------------
%% @doc
%% Function: hash_frag
%% Purpose: partition the db into Num parts and write them onto disk
%% @returns ok
%% @end
%%----------------------------------------------------------------------
-spec hash_frag(dl_db_instance(),
                [dl_rule()],
                integer(),
                integer(),
                pos_integer(),
                file:filename()) ->
                 ok.
hash_frag(DB, Rules, ProgNum, StageNum, TotTaskNum, DirPath) ->
  hash_frag_rec(DB, Rules, ProgNum, StageNum, 1, TotTaskNum, DirPath).

-spec get_part_cols_one_rule(dl_rule()) -> #{dl_atom() => [integer()]}.
get_part_cols_one_rule(Rule) ->
  case dl_repr:get_rule_body_atoms(Rule) of
    [A1 = #dl_atom{}, A2 = #dl_atom{}] ->
      {C1, C2} = eval:get_overlap_cols(A1#dl_atom.args, A2#dl_atom.args),
      #{dl_repr:get_atom_name(A1) => C1, dl_repr:get_atom_name(A2) => C2};
    [A] -> % when it is not a join rule, we partition on arbitrary col
      #{dl_repr:get_atom_name(A) => [1]}
  end.

-spec get_part_cols([dl_rule()]) -> #{dl_atom() => [integer()]}.
get_part_cols(Rules) ->
  lists:foldl(fun(Rule, AccIn) ->
                 NewCols = get_part_cols_one_rule(Rule),
                 maps:merge(AccIn, NewCols)
              end,
              #{},
              Rules).

%% @doc @returns a list of db_instances, each one of them representing the partitioned
%% instance that correspond to a particular task
%% len(return_list) == TotTaskNum
% TODO combine this with the other side effect frag
-spec hash_frag(dl_db_instance(), [dl_rule()], integer()) -> [dl_db_instance()].
hash_frag(DB, Rules, TotTasks) ->
  PartCols = get_part_cols(Rules),
  DBLists = dbs:fold(fun(Atom, AccIn) ->
              Cols =
                maps:get(
                  dl_repr:get_atom_name(Atom), PartCols, [1]),
              ToHash = dl_repr:get_atom_args_by_index(Cols, Atom),
              Hash = erlang:phash2(ToHash, TotTasks) + 1,
              {value, {N, DBN}} = lists:keysearch(Hash, 1, AccIn),
              lists:keyreplace(Hash, 1, AccIn, {N, dbs:add_element(Atom, DBN)})
           end,
           [{K, dbs:new()} || K <- lists:seq(1, TotTasks)],
           DB),
  lists:map(fun ({_N, L}) -> L end, DBLists).

  % lists:map(fun(N) ->
  %            dbs:filter(fun(Atom) ->
  %                          Cols =
  %                            maps:get(
  %                              dl_repr:get_atom_name(Atom), PartCols, [1]),
  %                          ToHash = dl_repr:get_atom_args_by_index(Cols, Atom),
  %                          N == erlang:phash2(ToHash, TotTasks) + 1
  %                       end,
  %                       DB)
  %         end,
  %         lists:seq(1, TotTasks)).
