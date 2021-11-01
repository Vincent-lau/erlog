-module(db_ops).

-compile(export_all).

-include("../include/data_repr.hrl").

%%----------------------------------------------------------------------
%% RA operations
%%----------------------------------------------------------------------


-spec product(dl_db_instance(), dl_db_instance()) -> dl_db_instance().
product(DB1, DB2) ->
  % TODO
  [].


-spec project(dl_db_instance(), [integer()]) -> dl_db_instance().
project(DBInstance, Cols) ->
  map(fun(Atom = #dl_atom{args = Args}) ->
         Atom#dl_atom{args = lists_filteri(fun(I) -> lists:member(I, Cols) end, Args)}
      end,
      DBInstance).

-spec join(dl_db_instance(), atom(), atom(), integer(), integer(), atom()) ->
            dl_db_instance().
join(Kb, PredSym1, PredSym2, C1, C2, ResName) ->
  Atoms1 = filter(fun(#dl_atom{pred_sym = Sym}) -> PredSym1 =:= Sym end, Kb),
  Atoms2 = filter(fun(#dl_atom{pred_sym = Sym}) -> PredSym2 =:= Sym end, Kb),
  flatmap(fun(Atom) -> join_one(Atoms2, C1, C2, ResName, Atom) end, Atoms1).

-spec join_one(dl_db_instance(), integer(), integer(), atom(), dl_atom()) ->
                dl_db_instance().
join_one(DlAtoms, C1, C2, ResName, #dl_atom{args = Args}) ->
  Nth = nth_arg(C1, Args),
  SelectedAtoms =
    filter(fun(#dl_atom{args = Args2}) -> nth_arg(C2, Args2) =:= Nth end, DlAtoms),
  SelectedArgs =
    map(fun(#dl_atom{args = Args2}) ->
           {L, [_ | R]} = split_args(C2 - 1, Args2),
           L ++ R
        end,
        SelectedAtoms),
  map(fun(Args2) -> #dl_atom{pred_sym = ResName, args = Args ++ Args2} end, SelectedArgs).

%%----------------------------------------------------------------------
%% operations on atoms in the db
%%----------------------------------------------------------------------

-spec get_rel_by_pred(atom(), dl_db_instance()) -> dl_db_instance().
get_rel_by_pred(Name, DBInstance) ->
  filter(fun(#dl_atom{pred_sym = N}) -> Name =:= N end, DBInstance).

-spec rename_pred(dl_db_instance(), dl_const()) -> dl_db_instance().
rename_pred(DB, NewPred) ->
  map(fun(A) -> A#dl_atom{pred_sym = NewPred} end, DB).

%%----------------------------------------------------------------------
%% operations on db representations
%%----------------------------------------------------------------------

lists_filteri(Predi, L) ->
  lists_filteri_rec(Predi, lists:zip(L, lists:seq(1, length(L)))).

lists_filteri_rec(Predi, [{E, Idx} | T]) ->
  case Predi(Idx) of
    true ->
      [E | lists_filteri_rec(Predi, T)];
    false ->
      lists_filteri_rec(Predi, T)
  end;
lists_filteri_rec(_, []) ->
  [].

-spec filteri(fun((dl_atom()) -> boolean()), dl_db_instance()) -> dl_db_instance().
filteri(Predi, Set) ->
  L = sets:to_list(Set),
  L2 = lists_filteri(Predi, L),
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

nth_arg(N, Args) ->
  lists:nth(N, Args).

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

-spec print_db(dl_db_instance()) -> atom().
print_db(DB) ->
  L = sets:to_list(DB),
  lists:foreach(fun(Atom) -> utils:ppt(Atom) end, L).
