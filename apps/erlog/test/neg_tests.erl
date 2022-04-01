-module(neg_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/data_repr.hrl").


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

neg_pred_test_() ->
  [{"Basic negation test", {setup, fun start/0, fun neg_link/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  Input = 
  "
    link(\"a\", \"b\").
    link(\"b\", \"c\").
    link(\"c\", \"c\").
    link(\"c\", \"d\").
    reachable(\"a\", \"b\").
    reachable(\"b\", \"c\").
    reachable(\"c\", \"c\").
    reachable(\"c\", \"d\").

    reachable(X, Y) :- link(X, Y).
    reachable(X, Y) :- link(X, Z), reachable(Z, Y).
  ",
  {F, _Rules} = preproc:lex_and_parse(str, Input),
  Facts = dbs:from_list(F),
  Atom = dl_repr:cons_atom("link", ["a", "b"]),
  {Atom, Facts}.

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

neg_link({Atom, EDB}) ->
  Negated = neg:neg_pred(Atom, EDB),
  Ans = dbs:from_list([
    dl_repr:cons_atom("link", ["a", "a"]),
    dl_repr:cons_atom("link", ["a", "c"]),
    dl_repr:cons_atom("link", ["a", "d"]),
    dl_repr:cons_atom("link", ["b", "a"]),
    dl_repr:cons_atom("link", ["b", "b"]),
    dl_repr:cons_atom("link", ["b", "d"]),
    dl_repr:cons_atom("link", ["c", "a"]),
    dl_repr:cons_atom("link", ["c", "b"]),
    dl_repr:cons_atom("link", ["d", "a"]),
    dl_repr:cons_atom("link", ["d", "b"]),
    dl_repr:cons_atom("link", ["d", "c"]),
    dl_repr:cons_atom("link", ["d", "d"])
  ]),
  ?debugFmt("Negated ~n~s~n Ans ~n~s~n", [dbs:to_string(Negated), dbs:to_string(Ans)]),
  ?_assert(dbs:equal(Negated, Ans)).
