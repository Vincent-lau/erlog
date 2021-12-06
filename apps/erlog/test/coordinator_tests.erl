-module(coordinator_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/coor_params.hrl").

-import(dl_repr, [get_atom_args_by_index/2, cons_atom/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

hash_and_write_one_test_() ->
  [{"Writing an atom to a file",
    {setup, fun start/0, fun stop/1, fun write_one_atom_correctly/1}}].


part_by_rules_test_() ->
  [{"Writing an atom to a file",
    {setup, fun start/0, fun stop/1, fun basic_part_with_tc_rules/1}}].


hash_frag_test_() ->
  [{"hash frag of tc program", 
    {setup, fun start2/0, fun hash_frag_of_tc/1}}].


%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  FileName = io_lib:format("~stask-~s", [?INTER_DIR, "foo"]),
  {ok, Stream} = file:open(FileName, [write, read]),
  Stream.

start2() -> ok.

stop(Stream) ->
  file:close(Stream).

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%


hash_frag_of_tc(_) ->
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
  {Rules, F} = preproc:lex_and_parse(Input),
  Facts = db_ops:from_list(F),
  coordinator:hash_frag(Facts, Rules, ?NUM_TASKS),
  
  ?_assert(true).

basic_part_with_tc_rules(Stream) ->
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
  {Rules, F} = preproc:lex_and_parse(Input),
  Facts = db_ops:from_list(F),
  A = cons_atom("reachable", ["b", "c"]),
  Num = erlang:phash2(get_atom_args_by_index([2], A), ?NUM_TASKS) + 1,
  coordinator:part_by_rules(Facts, Rules, Num, Stream),
  file:position(Stream, bof),
  Line1 = io:get_line(Stream, ''),
  Line2 = io:get_line(Stream, ''),
  Line3 = io:get_line(Stream, ''),
  Line4 = io:get_line(Stream, ''),
  S1 = sets:from_list([Line1, Line2, Line3, Line4]),
  SExp = sets:from_list([
    "link(\"b\", \"c\").\n",
    "link(\"c\", \"c\").\n",
    "reachable(\"c\", \"d\").\n",
    "reachable(\"c\", \"c\").\n"
  ]),
  ?_assertEqual(SExp, S1).


write_one_atom_correctly(Stream) ->
  Atom = dl_repr:cons_atom("reachable", ["a", "b"]),
  TaskNum = erlang:phash2(get_atom_args_by_index([2], Atom), ?NUM_TASKS) + 1,
  coordinator:hash_and_write_one(Atom, [2], TaskNum, Stream),
  file:position(Stream, bof),
  Line = io:get_line(Stream, ''),
  ?_assertEqual("reachable(\"a\", \"b\").\n", Line).