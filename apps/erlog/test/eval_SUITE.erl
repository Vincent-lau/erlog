-module(eval_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([tc_tests/1, tc_large_tests/1, rsg_tests/1, marrying_a_widower_tests/1,
         nonlinear_ancestor_test/1]).

-import(dl_repr, [cons_atom/2, cons_const/1]).

all() ->
  [tc_tests, tc_large_tests, rsg_tests, marrying_a_widower_tests, nonlinear_ancestor_test].

init_per_testcase(tc_tests, Config) ->
  TabId = ets:new(dl_atom_names, [named_table]),
  [{table, TabId} | Config];
init_per_testcase(tc_large_tests, Config) ->
  TabId = ets:new(dl_atom_names, [named_table]),
  [{table, TabId} | Config];
init_per_testcase(nonlinear_ancestor_test, Config) ->
  TabId = ets:new(dl_atom_names, [named_table]),
  [{table, TabId} | Config];
init_per_testcase(rsg_tests, Config) ->
  TabId = ets:new(dl_atom_names, [named_table]),
  [{table, TabId} | Config];
init_per_testcase(marrying_a_widower_tests, Config) ->
  TabId = ets:new(dl_atom_names, [named_table]),
  [{table, TabId} | Config].

end_per_testcase(tc_tests, Config) ->
  ets:delete(?config(table, Config));
end_per_testcase(tc_large_tests, Config) ->
  ets:delete(?config(table, Config));
end_per_testcase(nonlinear_ancestor_test, Config) ->
  ets:delete(?config(table, Config));
end_per_testcase(rsg_tests, Config) ->
  ets:delete(?config(table, Config));
end_per_testcase(marrying_a_widower_tests, Config) ->
  ets:delete(?config(table, Config)).

eval_tests(Config, ProgName, QryName) ->
  % open file and read program
  {ok, Stream} = file:open(?config(data_dir, Config) ++ ProgName ++ ".dl", [read]),
  {Facts, Rules} = preproc:lex_and_parse(Stream),
  file:close(Stream),
  % preprocess rules
  Prog2 = preproc:process(Rules),
  ct:pal("Input program is:~n~s~n", [utils:to_string(Prog2)]),

  ct:pal("input data is ~p~n", [Facts]),
  % create EDB from input relations
  EDB = db_ops:from_list(Facts),
  Res = eval:eval_all(Prog2, EDB),
  ct:pal("Total result db is~n~s~n", [db_ops:db_to_string(Res)]),
  ResQ = db_ops:get_rel_by_pred(cons_const(QryName), Res),
  {ok, Stream3} = file:open(?config(data_dir, Config) ++ QryName ++ ".csv", [read]),
  Output = read_data(Stream3),
  Ans = cons_db_from_data(Output, QryName),
  ct:pal("The result database is:~n~s~n and the ans db is ~n~s~n",
         [db_ops:db_to_string(ResQ), db_ops:db_to_string(Ans)]),
  true = db_ops:equal(Ans, ResQ).

tc_tests(Config) ->
  eval_tests(Config, "tc", "reachable").

tc_large_tests(Config) ->
  eval_tests(Config, "tc-large", "tc_large").

nonlinear_ancestor_test(Config) ->
  eval_tests(Config, "non-linear-ancestor", "ancestor").

rsg_tests(Config) ->
  eval_tests(Config, "rsg", "rsg").

marrying_a_widower_tests(Config) ->
  eval_tests(Config, "marrying-a-widower", "grandfather").

read_data(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = erl_scan:string(Line),
      [lists:map(fun({_, _, Args}) -> Args end, Tokens) | read_data(S)]
  end.

cons_db_from_data(Data, AtomName) ->
  db_ops:from_list(
    lists:map(fun(Args) -> cons_atom(AtomName, Args) end, Data)).
