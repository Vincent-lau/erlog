-module(preproc).

-compile(export_all).

-include("../include/data_repr.hrl").

% partition rules with more than two body atoms into a list of
% smaller rules e.g.
%
% a(x,y,z) :- b(w,x), c(x,y), d(y,z). is turned into
% a(x,y,z) :- b(w,x), int(x,y,z) and int(x,y,z) :- c(x,y), d(y,z).
-spec rule_part(dl_rule()) -> [dl_rule()].
rule_part(R = #dl_rule{body = Body}) ->
  case Body of
    [H1, H2, H3 | T] ->
      % 1. generate a list of args that contain all terms in the rest of the atoms
      RuleRest = collect_args([H2, H3 | T]),
      R1 = #dl_rule{head = R#dl_rule.head, body = [H1, RuleRest#dl_rule.head]},
      [R1 | rule_part(RuleRest)];
    _ ->
      [R]
  end.

%%----------------------------------------------------------------------
%% Function:
%% Purpose: Takes a list of atoms, generate a head atom that has all the args
%% and the head atom has a random name
%% Args: A list of atoms
%% Returns: A rule
%%----------------------------------------------------------------------
-spec collect_args([dl_atom()]) -> dl_rule().
collect_args(Atoms) ->
  HeadName = random_name(),
  ArgsL = lists:flatmap(fun(#dl_atom{args = A}) -> A end, Atoms),
  % HACK deduplicate cannot preserve element order so sort it to get "some" order
  Args =
    lists:sort(
      sets:to_list(
        sets:from_list(ArgsL))),
  Head = dl_repr:cons_atom(HeadName, Args),
  dl_repr:cons_rule(Head, Atoms).

-spec random_name() -> string().
random_name() ->
  N = case ets:lookup(table, counter) of
        [] ->
          0;
        [{_, X}] ->
          X
      end,
  Name = "_name" ++ integer_to_list(N),
  ets:insert(table, {counter, N + 1}),
  Name.
