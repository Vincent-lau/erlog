-module(neg_tests).

-include_lib("eunit/include/eunit.hrl").


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

neg_pred_test_() ->
  [{"Basic negation test", {setup, fun start/0, fun neg_link/1}}].

is_stratifiable_test_() ->
  [{"cycle must contain negation", {setup, fun start2/0, fun cycle_no_negation/1}},
  {"one negative edge in the cycle means no stratifiable", 
    {setup, fun start3/0, fun one_neg_edge/1}}].

compute_stratification_test_() ->
  [{"compute stratification for unreachable", {setup, fun start4/0, fun unreachable_prog/1}}].

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

start2() ->
  Input = 
  "
    reachable(X, Y) :- link(X, Y).
    reachable(X, Y) :- link(X, Z), reachable(Z, Y).
    indirect(X, Y) :- reachable(X, Y), !link(X, Y).
  ",
  {_F, Program} = preproc:lex_and_parse(str, Input),
  Program.

start3() ->
  Input =
  "
    p(X) :- !q(X).
    q(X) :- p(X).
  ",
  {_F, Program} = preproc:lex_and_parse(str, Input),
  Program.


start4() ->
  Input =
  "
    reachable(X,Y) :- link(X,Y).
    reachable(X,Y) :- link(X,Z), reachable(Z,Y).
    node(X) :- link(X,Y).
    node(Y) :- link(X,Y).
    unreachable(X,Y) :- node(X), node(Y), !reachable(X,Y).
  ",
  {_F, Program} = preproc:lex_and_parse(str, Input),
  Program.

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
  ?_assert(dbs:equal(Negated, Ans)).

cycle_no_negation(Program) ->
  ?_assert(neg:is_stratifiable(Program)).

one_neg_edge(Program) ->
  ?_assert(not neg:is_stratifiable(Program)).

unreachable_prog(Program) ->
  Strata = neg:compute_stratification(Program),
  PS1 = 
  "
    reachable(X,Y) :- link(X,Y).
    reachable(X,Y) :- link(X,Z), reachable(Z,Y).
  ",
  PS2 = "
    node(X) :- link(X,Y).
    node(Y) :- link(X,Y).
  ",
  {_F1, P1} = preproc:lex_and_parse(str, PS1),
  {_F2, P2} = preproc:lex_and_parse(str, PS2),
  {_F3, P3} = preproc:lex_and_parse(str, "unreachable(X,Y) :- node(X), node(Y), !reachable(X,Y)."),
  ?_assertEqual([P1, P2, P3], Strata).
  
  