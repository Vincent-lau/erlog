-module(preproc).

-compile(export_all).

-include("../include/data_repr.hrl").

-spec lex_and_parse(file:filename() | file:io_device()) -> {[dl_atom()], [dl_rule()]}.
lex_and_parse(Str) when is_list(Str) ->
  {ok, Tokens, _} = dl_lexer:string(Str),
  {ok, Prog} = dl_parser:parse(Tokens),
  Rules = lists:filter(fun dl_repr:is_dl_rule/1, Prog),
  Facts = lists:filter(fun dl_repr:is_dl_atom/1, Prog),
  {Facts, Rules};
lex_and_parse(Stream) when is_pid(Stream) or is_atom(Stream) ->
  Tokens = read_and_lex(Stream),
  {ok, Prog} = dl_parser:parse(Tokens),
  Facts = lists:filter(fun dl_repr:is_dl_atom/1, Prog),
  Rules = lists:filter(fun dl_repr:is_dl_rule/1, Prog),
  {Facts, Rules}.

read_and_lex(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = dl_lexer:string(Line),
      Tokens ++ read_and_lex(S)
  end.

-spec process_rules(dl_program()) -> dl_program().
process_rules(Prog) ->
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
%% Purpose: Takes a list of list of args, flatten the list and unique all
%% args according to the order they appear
%% Args: A list of lists of args
%% Returns: A flattened list with unique args
%%
%% Example: [[c, b, a], [a, b, d]] -> [c, b, a, d]
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
  N = case ets:lookup(dl_atom_names, counter) of
        [] ->
          0;
        [{_, X}] ->
          X
      end,
  Name = "_name" ++ integer_to_list(N),
  ets:insert(dl_atom_names, {counter, N + 1}),
  Name.
