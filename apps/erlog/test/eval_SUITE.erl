-module(eval_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, groups/0]).
-export([tc_tests/1, tc2_tests/1, tc_large_tests/1, rsg_tests/1,
         marrying_a_widower_tests/1, nonlinear_ancestor_tests/1, scc_tests/1, pointsto_tests/1,
         indirect_tests/1, unreachable_tests/1]).

-import(dl_repr, [cons_atom/2, cons_const/1]).

all() ->
  [{group, positive_datalog}, {group, negative_datalog}].

groups() ->
  [{positive_datalog,
    [shuffle],
    [tc_tests,
     tc2_tests,
     rsg_tests,
     tc_large_tests,
     marrying_a_widower_tests,
     scc_tests,
     nonlinear_ancestor_tests,
     pointsto_tests]},
   {negative_datalog, [shuffle], [indirect_tests, unreachable_tests]}].

ets_owner() ->
  receive
    stop ->
      exit(normal);
    _Other ->
      ets_owner()
  end.

init_per_suite(Config) ->
  lager:start(),
  Pid = spawn(fun ets_owner/0),
  TabId = ets:new(dl_atom_names, [named_table, public, {heir, Pid, []}]),
  ProgramDir = ?config(data_dir, Config) ++ "../test_program/",
  [{table, TabId}, {table_owner, Pid}, {program_dir, ProgramDir} | Config].

end_per_suite(Config) ->
  ?config(table_owner, Config) ! stop.

eval_tests(Config, ProgName) ->
  % open file and read program
  {Facts, Rules} = preproc:lex_and_parse(file, ?config(program_dir, Config) ++ ProgName),
  QryNames = preproc:get_output_name(?config(program_dir, Config) ++ ProgName),
  % preprocess rules
  Prog2 = preproc:process_rules(Rules),
  ct:pal("Input program is:~n~s~n", [dl_repr:program_to_string(Prog2)]),

  ct:pal("input data is ~p~n", [Facts]),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),
  Res = eval:eval_all(Prog2, EDB),
  ct:pal("Total result db is~n~s~n", [dbs:to_string(Res)]),
  ResQL = lists:map(fun(QryName) -> dbs:get_rel_by_name(QryName, Res) end, QryNames),
  AnsL =
    lists:map(fun(QryName) ->
                 dbs:read_db(?config(program_dir, Config) ++ QryName ++ ".csv", QryName)
              end,
              QryNames),
  ct:pal("The result database is:~n~s~n and the ans db is ~n~s~n",
         [lists:map(fun dbs:to_string/1, ResQL), lists:map(fun dbs:to_string/1, AnsL)]),

  ct:pal("ResQ - Ans ~n~s~n, Ans-ResQ ~n~s~n",
         [dbs:to_string(
            dbs:subtract(hd(ResQL), hd(AnsL))),
          dbs:to_string(
            dbs:subtract(hd(AnsL), hd(ResQL)))]),

  true = lists:all(fun({ResQ, Ans}) -> dbs:equal(ResQ, Ans) end, lists:zip(ResQL, AnsL)).

tc_tests(Config) ->
  eval_tests(Config, "tc.dl").

tc2_tests(Config) ->
  eval_tests(Config, "tc2.dl").

tc_large_tests(Config) ->
  eval_tests(Config, "tc-large.dl").

nonlinear_ancestor_tests(Config) ->
  eval_tests(Config, "non-linear-ancestor.dl").

rsg_tests(Config) ->
  eval_tests(Config, "rsg.dl").

scc_tests(Config) ->
  eval_tests(Config, "scc.dl").

marrying_a_widower_tests(Config) ->
  eval_tests(Config, "marrying-a-widower.dl").

pointsto_tests(Config) ->
  eval_tests(Config, "pointsto.dl").

indirect_tests(Config) ->
  eval_tests(Config, "indirect.dl").

unreachable_tests(Config) ->
  eval_tests(Config, "unreachable.dl").
