-module(preproc).

-compile(export_all).

-include("../include/data_repr.hrl").

-spec process(dl_program()) -> dl_program().
process(Prog) ->
  lists:flatmap(fun rule_part/1, Prog).

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
      RuleRest = combine_atoms([H2, H3 | T]),
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
-spec combine_atoms([dl_atom()]) -> dl_rule().
combine_atoms(Atoms) ->
  HeadName = random_name(),
  combine_atoms(HeadName, Atoms).

combine_atoms(HeadName, Atoms) ->
  Args = combine_args(lists:map(fun(#dl_atom{args = A}) -> A end, Atoms)),
  Head = dl_repr:cons_atom(HeadName, Args),
  dl_repr:cons_rule(Head, Atoms).

%%----------------------------------------------------------------------
%% Function:
%% Purpose: Takes a list of atoms, generate a head atom that has all the args
%% and the head atom has a random name
%% Args: A list of atoms
%% Returns: A rule
%%----------------------------------------------------------------------
-spec combine_args([[dl_term()]]) -> [dl_term()].
combine_args(ArgsL) ->
  AllArgs = lists:flatmap(fun(X) -> X end, ArgsL),
  L = lists:foldl(fun(E, Acc) ->
                     case lists:member(E, Acc) of
                       true -> Acc;
                       false -> [E | Acc]
                     end
                  end,
                  [],
                  AllArgs),
  lists:reverse(L).

  % HACK deduplicate cannot preserve element order so sort it to get "some" order

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
