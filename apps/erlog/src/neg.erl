-module(neg).

-include("../include/data_repr.hrl").

-compile(export_all).

-type domain() :: gb_sets:set(dl_const()).

-spec neg_pred(dl_atom(), dl_db_instance()) -> dl_db_instance().
neg_pred(Atom, EDB) ->
  % generate all possible predicates, then subtract from the EDB
  Arity = dl_repr:get_atom_arity(Atom),
  AtomName = dl_repr:get_atom_name(Atom),
  Domain = active_domain(EDB, AtomName),
  AllAtoms = gen_all_from_domain(AtomName, Arity, Domain),
  ExistingAtoms = dbs:get_rel_by_pred(AtomName, EDB),
  Smallest = gb_sets:smallest(ExistingAtoms),
  S = gb_sets:smallest(element(2, gb_sets:take_smallest(AllAtoms))),
  io:format(standard_error, "edb ~n~p~n", [EDB]),
  io:format(standard_error, "all atoms ~n~p~n", [AllAtoms]),
  io:format(standard_error, "existing atoms ~n~p~n", [ExistingAtoms]),
  io:format(standard_error, "all - existing ~n~s~n", [dbs:to_string(dbs:subtract(AllAtoms, ExistingAtoms))]),
  io:format(standard_error, "minimum1 ~p~n minimum2 ~p~n", [Smallest, S]),
  io:format(standard_error, "smallest element in AllAtoms ~p~n", [gb_sets:is_element(Smallest, AllAtoms)]),
  dbs:subtract(AllAtoms, ExistingAtoms).

-spec gen_all_from_domain(string(), integer(), domain()) -> dl_db_instance().
gen_all_from_domain(AtomName, Arity, Domain) ->
  gen_all_rec(AtomName, Arity, Domain, []).

-spec gen_all_rec(string(), integer(), domain(), [dl_const()]) -> dl_db_instance().
gen_all_rec(AtomName, Arity, _Domain, CurArgs) when length(CurArgs) == Arity ->
  dbs:singleton(
    dl_repr:cons_atom(AtomName, lists:reverse(CurArgs)));
gen_all_rec(AtomName, Arity, Domain, CurArgs) when length(CurArgs) < Arity ->
  gb_sets:fold(fun(Const, Acc) ->
                  dbs:union(Acc, gen_all_rec(AtomName, Arity, Domain, [Const | CurArgs]))
               end,
               dbs:new(),
               Domain).

%%----------------------------------------------------------------------
%% @doc
%% The active domain of a database instance I, denoted adom(I), is the set of
%% all constants occurring in I, and the active domain adom(I) of relation instance
%% I is defined analogously.
%% @end
%%----------------------------------------------------------------------
-spec active_domain(dl_db_instance(), string()) -> domain().
active_domain(EDB, AtomName) ->
  % TODO we want to find the domain for one atom or the whole db instance?
  Atoms = dbs:get_rel_by_pred(AtomName, EDB),
  AllArgs = dbs:map(fun dl_repr:get_atom_args/1, Atoms),
  dbs:fold(fun(Args, Acc) -> gb_sets:union(Acc, gb_sets:from_list(Args)) end,
              gb_sets:new(),
              AllArgs).

