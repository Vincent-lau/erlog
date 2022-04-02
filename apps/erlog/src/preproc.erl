-module(preproc).

-export([lex_and_parse/2, process_rules/1, combine_args/1, combine_atoms/1, rule_part/1,
         get_output_name/2, get_output_name/1]).

-include("../include/data_repr.hrl").

-include_lib("kernel/include/logger.hrl").


-spec get_output_name(file:filename()) -> [string()]  | no_output.
get_output_name(Input) ->
  get_output_name(file, Input).

-spec get_output_name(InputType, Input) -> [string()] | no_output
  when InputType :: file | stream | str,
       Input :: file:io_device() | file:filename() | string().
get_output_name(file, FName) ->
  {ok, Stream} = file:open(FName, [read]),
  Res = get_output_name(stream, Stream),
  file:close(Stream),
  Res;
get_output_name(stream, S) ->
  case io:get_line(S, "") of
    eof ->
      [];
    L ->
      case get_output_name(str, L) of
        no_output ->
          get_output_name(stream, S);
        OutName ->
          [OutName | get_output_name(stream, S)]
      end
  end;
get_output_name(str, Str) ->
  SList = string:lexemes(Str, "\n"),
  case lists:filter(fun is_output_line/1, SList) of
    [H] ->
      [_H1, H2] = string:lexemes(H, " "),
      H2;
    [] ->
      no_output;
    _Other ->
      exit(invalid_lexeme)
  end.

is_output_line(S) ->
  case string:lexemes(S, " ") of
    L when length(L) == 2 andalso hd(L) =:= ".output" ->
      true;
    _Other ->
      false
  end.

%%----------------------------------------------------------------------
%% @doc
%% Given a file name as a string or a stream or a string of the program
%% to be parsed, lex and parse the program from the file.
%%
%% @returns a program consisting of a list of rules and facts with a list
%% of atoms
%% @end
%%----------------------------------------------------------------------
-spec lex_and_parse(InputType, Input) -> {[dl_atom()], [dl_rule()]}
  when InputType :: file | stream | str,
       Input :: file:io_device() | file:filename() | string().
lex_and_parse(file, FName) ->
  {ok, Stream} = file:open(FName, [read]),
  Res = lex_and_parse(Stream),
  file:close(Stream),
  Res;
lex_and_parse(stream, S) ->
  lex_and_parse(S);
lex_and_parse(str, Str) ->
  lex_and_parse(Str).

%%----------------------------------------------------------------------
%% @doc
%% This function is the wrapped by `lex_and_parse/2' defined above, it
%% implements two cases where the input is a literal string to be parsed
%% and a `file:io_device()'.
%% @end
%%----------------------------------------------------------------------
-spec lex_and_parse(file:filename() | file:io_device()) -> {[dl_atom()], [dl_rule()]}.
lex_and_parse(Str) when is_list(Str) ->
  {ok, Tokens, _} = dl_lexer:string(Str),
  {ok, Prog} = dl_parser:parse(Tokens),
  Rules = lists:filter(fun dl_repr:is_dl_rule/1, Prog),
  Facts = lists:filter(fun dl_repr:is_dl_atom/1, Prog),
  {Facts, Rules};
lex_and_parse(Stream) when is_pid(Stream) or is_atom(Stream) ->
  Tokens = read_and_lex(Stream),
  % work around: allow empty file although yecc does not support this
  case Tokens of
    [] ->
      {[], []};
    _T ->
      try dl_parser:parse(Tokens) of
        {ok, Prog} ->
          Facts = lists:filter(fun dl_repr:is_dl_atom/1, Prog),
          Rules = lists:filter(fun dl_repr:is_dl_rule/1, Prog),
          {Facts, Rules}
      catch
        error:Error ->
          ?LOG_WARNING(#{error_caused_by_token => Tokens}),
          {error, caught, Error}
      end
  end.

read_and_lex(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = dl_lexer:string(Line),
      case length(Tokens) of
        0 ->
          read_and_lex(S);
        _N ->
          case lists:last(Tokens) of
            {'.', _} ->
              Tokens ++ read_and_lex(S);
            _Other -> % this is incomplete results, so just ignore it
              read_and_lex(S)
          end
      end
  end.

%%----------------------------------------------------------------------
%% @doc
%% process rules so that all of them consist of two body atoms at most
%% @returns a list of processed rules.
%% @end
%%----------------------------------------------------------------------
-spec process_rules(dl_program()) -> dl_program().
process_rules(Prog) ->
  lists:flatmap(fun rule_part/1, Prog).

%%----------------------------------------------------------------------
%% @doc
%% partition rules with more than two body atoms into a list of
%% smaller rules e.g.
%%
%% a(x,y,z) :- b(w,x), c(x,y), d(y,z). is turned into
%% a(x,y,z) :- b(w,x), int(x,y,z) and int(x,y,z) :- c(x,y), d(y,z).
%% @end
%%----------------------------------------------------------------------
-spec rule_part(dl_rule()) -> [dl_rule()].
rule_part(R = #dl_rule{}) ->
  case dl_repr:get_rule_body_atoms(R) of
    [H1, H2, H3 | T] ->
      % 1. generate a list of args that contain all terms in the rest of the atoms
      RuleRest = combine_atoms([H2, H3 | T]),
      R1 =
        dl_repr:cons_rule(
          dl_repr:get_rule_head(R), [H1, dl_repr:get_rule_head(RuleRest)]),
      [R1 | rule_part(RuleRest)];
    _ ->
      [R]
  end.

%%----------------------------------------------------------------------
%% @doc
%% Purpose: Takes a list of atoms, generate a head atom that has all the args
%% and the head atom has a random name
%% Args: A list of atoms
%% @returns: A rule
%% @end
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
%% @doc
%% Purpose: Takes a list of list of args, flatten the list and unique all
%% args according to the order they appear
%% Args: A list of lists of args
%% @returns: A flattened list with unique args
%%
%% Example: [[c, b, a], [a, b, d]] -> [c, b, a, d]
%% @end
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
  Name = "tmpname_" ++ integer_to_list(N),
  ets:insert(dl_atom_names, {counter, N + 1}),
  Name.
