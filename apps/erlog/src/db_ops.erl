-module(db_ops).

-compile(export_all).

-include("../include/data_repr.hrl").
-include("../include/utils.hrl").

%%----------------------------------------------------------------------
%% RA operations
%%----------------------------------------------------------------------

-spec project(dl_db_instance(), [integer()]) -> dl_db_instance().
project(DBInstance, Cols) ->
  ?LOG_TOPIC_DEBUG(db_ops_project,
                   #{projection_cols => Cols,
                     db_pre_projection => db_ops:db_to_string(DBInstance)}),
  map(fun(Atom = #dl_atom{args = Args}) ->
         Atom#dl_atom{args = listsi:filteri(fun(_, I) -> lists:member(I, Cols) end, Args)}
      end,
      DBInstance).

-spec join(dl_db_instance(), atom(), atom(), [integer()], [integer()], atom()) ->
            dl_db_instance().
join(Kb, PredSym1, PredSym2, C1, C2, ResName) ->
  Atoms1 = filter(fun(#dl_atom{pred_sym = Sym}) -> PredSym1 =:= Sym end, Kb),
  Atoms2 = filter(fun(#dl_atom{pred_sym = Sym}) -> PredSym2 =:= Sym end, Kb),
  flatmap(fun(Atom) -> join_one(Atoms2, Atom, C1, C2, ResName) end, Atoms1).

-spec join_one(dl_db_instance(), dl_atom(), [integer()], [integer()], dl_const()) ->
                dl_db_instance().
join_one(DlAtoms, #dl_atom{args = Args1}, C1, C2, ResName) ->
  SelectedAtoms =
    filter(fun(#dl_atom{args = Args2}) ->
              lists:sort(nth_args(C2, Args2)) =:= lists:sort(nth_args(C1, Args1))
           end,
           DlAtoms),
  % ?LOG_TOPIC_DEBUG(join_one, "dl atoms are ~s~n", [db_ops:db_to_string(DlAtoms)]),
  % ?LOG_TOPIC_DEBUG(join_one, "input atom has args ~p~n", [Args1]),
  % ?LOG_TOPIC(join_one,
  %            debug,
  %            "selected atoms are ~s~n",
  %            [db_ops:db_to_string(SelectedAtoms)]),
  RemoveJoinColArgs =
    map(fun(A = #dl_atom{args = Args2}) ->
           NewArgs = listsi:filteri(fun(_, I) -> not lists:member(I, C2) end, Args2),
           A#dl_atom{args = NewArgs}
        end,
        SelectedAtoms),
  map(fun(#dl_atom{args = Args2}) -> #dl_atom{pred_sym = ResName, args = Args1 ++ Args2}
      end,
      RemoveJoinColArgs).

%%----------------------------------------------------------------------
%% operations on atoms in the db
%%----------------------------------------------------------------------

-spec get_rel_by_pred(dl_const(), dl_db_instance()) -> dl_db_instance().
get_rel_by_pred(Name, DBInstance) ->
  filter(fun(#dl_atom{pred_sym = N}) -> Name =:= N end, DBInstance).

-spec rename_pred(dl_const(), dl_db_instance()) -> dl_db_instance().
rename_pred(NewPred, DB) ->
  map(fun(A) -> A#dl_atom{pred_sym = NewPred} end, DB).

%%----------------------------------------------------------------------
%% operations on db representations
%%----------------------------------------------------------------------

-spec filteri(fun((dl_atom()) -> boolean()), dl_db_instance()) -> dl_db_instance().
filteri(Predi, Set) ->
  L = sets:to_list(Set),
  L2 = listsi:filteri(Predi, L),
  sets:from_list(L2).

-spec map(fun((dl_atom()) -> dl_atom()), dl_db_instance()) -> dl_db_instance().
map(Fun, Set) ->
  L = sets:to_list(Set),
  L2 = lists:map(Fun, L),
  sets:from_list(L2).

%% input Set would be a set of sets
-spec flatmap(fun((term()) -> term()), sets:set()) -> sets:set().
flatmap(Fun, Set) ->
  S2 = map(Fun, Set),
  sets:union(
    sets:to_list(S2)).

filter(Pred, Set) ->
  sets:filter(Pred, Set).

nth_args([], _) ->
  [];
nth_args([H | T], Args) ->
  [lists:nth(H, Args) | nth_args(T, Args)].

split_args(N, Args) ->
  lists:split(N, Args).

-spec equal(dl_db_instance(), dl_db_instance()) -> boolean().
equal(DB1, DB2) ->
  DB1 =:= DB2.

-spec add_db_unique(dl_db_instance(), dl_db_instance()) -> dl_db_instance().
add_db_unique(CurDB, NewDB) ->
  sets:union(CurDB, NewDB).

-spec from_list([dl_atom()]) -> dl_db_instance().
from_list(L) ->
  sets:from_list(L).

-spec flatten([dl_db_instance()]) -> dl_db_instance().
flatten(L) ->
  sets:union(L).

-spec db_to_string(dl_db_instance()) -> string().
db_to_string(DB) ->
  L = sets:to_list(DB),
  lists:join("\n", lists:map(fun(Atom) -> utils:to_string(Atom) end, L)).
