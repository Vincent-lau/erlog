-module(db_ops).

-compile(export_all).

-include("../include/data_repr.hrl").

%%----------------------------------------------------------------------
%% RA operations
%%----------------------------------------------------------------------

-spec product(dl_db_instance(), dl_const(), dl_const()) -> dl_db_instance().
product(DB, PredSym1, PredSym2) ->
  % TODO
  [].

-spec project(dl_db_instance(), [integer()]) -> dl_db_instance().
project(DBInstance, Cols) ->
  map(fun(Atom = #dl_atom{args = Args}) ->
         Atom#dl_atom{args = listsi:filteri(fun(_, I) -> lists:member(I, Cols) end, Args)}
      end,
      DBInstance).

-spec join(dl_db_instance(), atom(), atom(), [integer()], [integer()], atom()) ->
            dl_db_instance().
join(Kb, PredSym1, PredSym2, C1, C2, ResName) ->
  Atoms1 = filter(fun(#dl_atom{pred_sym = Sym}) -> PredSym1 =:= Sym end, Kb),
  Atoms2 = filter(fun(#dl_atom{pred_sym = Sym}) -> PredSym2 =:= Sym end, Kb),
  utils:dbg_format("knowledgebase is ~s sym1 is ~p sym2 is ~p~n", [db_ops:db_to_string(Kb), PredSym1, PredSym2]),
  flatmap(fun(Atom) -> join_one(Atoms2, Atom, C1, C2, ResName) end, Atoms1).

-spec join_one(dl_db_instance(), dl_atom(), [integer()], [integer()], dl_const()) ->
                dl_db_instance().
join_one(DlAtoms, #dl_atom{args = Args}, C1, C2, ResName) ->
  SelectedAtoms =
    filter(fun(#dl_atom{args = Args2}) ->
              lists:sort(nth_args(C2, Args2)) =:= lists:sort(nth_args(C1, Args))
           end,
           DlAtoms),
  utils:dbg_format("dlatoms are ~s~n", [db_ops:db_to_string(DlAtoms)]),
  utils:dbg_format("nth args ~p~n", [nth_args(C1, Args)]),
  utils:dbg_format("selected atoms are ~s~n", [db_ops:db_to_string(SelectedAtoms)]),
  SelectedArgs =
    map(fun(#dl_atom{args = Args2}) ->
           lists:filter(fun(A) -> not lists:member(A, Args) end, Args2)
        end,
        SelectedAtoms),
  map(fun(Args2) -> #dl_atom{pred_sym = ResName, args = Args ++ Args2} end, SelectedArgs).

%%----------------------------------------------------------------------
%% operations on atoms in the db
%%----------------------------------------------------------------------

-spec get_rel_by_pred(dl_db_instance(), dl_const()) -> dl_db_instance().
get_rel_by_pred(DBInstance, Name) ->
  filter(fun(#dl_atom{pred_sym = N}) -> Name =:= N end, DBInstance).

-spec rename_pred(dl_db_instance(), dl_const()) -> dl_db_instance().
rename_pred(DB, NewPred) ->
  map(fun(A) -> A#dl_atom{pred_sym = NewPred} end, DB).

%%----------------------------------------------------------------------
%% operations on db representations
%%----------------------------------------------------------------------

-spec filteri(fun((dl_atom()) -> boolean()), dl_db_instance()) -> dl_db_instance().
filteri(Predi, Set) ->
  L = sets:to_list(Set),
  L2 = listsi:filteri(Predi, L),
  sets:from_list(L2).

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
