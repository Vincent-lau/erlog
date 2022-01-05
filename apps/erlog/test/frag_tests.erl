-module(frag_tests).

-include_lib("eunit/include/eunit.hrl").

-import(dl_repr, [get_atom_args_by_index/2, cons_atom/2]).

-define(num_tasks, 4).
-define(inter_dir, "tmp/").

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
    {setup, fun start/0, fun stop/1, fun hash_frag_of_tc/1}}].


%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%

start() ->
  case filelib:is_dir(?inter_dir) of
    true ->
      file:del_dir_r(?inter_dir);
    false ->
      ok
  end,
  ok = file:make_dir(?inter_dir),
  FileName = io_lib:format("~s-~s", [?inter_dir ++ "task", "foo"]),
  {ok, Stream} = file:open(FileName, [write, read]),
  Stream.


stop(Stream) ->
  file:close(Stream),
  file:del_dir_r("tmp/").

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%


hash_frag_of_tc(_S) ->
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
  {F, Rules} = preproc:lex_and_parse(str, Input),
  Facts = dbs:from_list(F),
  frag:hash_frag(Facts, Rules, ?num_tasks, 1, ?inter_dir),
  
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
  {F, Rules} = preproc:lex_and_parse(str, Input),
  Facts = dbs:from_list(F),
  A = cons_atom("reachable", ["b", "c"]),
  Num = erlang:phash2(get_atom_args_by_index([2], A), ?num_tasks) + 1,
  frag:part_by_rules(Facts, Rules, Num, ?num_tasks, Stream),
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
  TaskNum = erlang:phash2(get_atom_args_by_index([2], Atom), ?num_tasks) + 1,
  frag:hash_and_write_one(Atom, [2], TaskNum, ?num_tasks, Stream),
  file:position(Stream, bof),
  Line = io:get_line(Stream, ''),
  ?_assertEqual("reachable(\"a\", \"b\").\n", Line).
