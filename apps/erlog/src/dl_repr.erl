-module(dl_repr).

-export([cons_const/1, cons_atom/2, cons_rule/2, cons_term/1, cons_args_from_list/1]).
-export([get_atom_args/1, get_atom_name/1, get_rule_head/1, get_rule_headname/1,
         get_rule_body/1, get_atom_args_by_index/2]).
-export([const_to_string/1, var_to_string/1, atoms_to_string/1, atom_to_string/1,
        rule_to_string/1, rules_to_string/1, program_to_string/1]).
-export([is_dl_atom/1, is_dl_rule/1, is_dl_const/1, is_dl_var/1]).

-include("../include/data_repr.hrl").

-spec is_dl_atom(any()) -> boolean().
is_dl_atom(A) when is_tuple(A) ->
  element(1, A) =:= dl_atom;
is_dl_atom(_) ->
  false.

-spec is_dl_rule(any()) -> boolean().
is_dl_rule(R) when is_tuple(R) ->
  element(1, R) =:= dl_rule;
is_dl_rule(_) ->
  false.

-spec cons_const(string() | atom()) -> dl_const().
cons_const(S) when is_list(S)->
  list_to_atom(S);
cons_const(A) when is_atom(A) ->
  A.

-spec cons_atom(string(), [dl_term() | string()]) -> dl_atom().
cons_atom(PredSym, TermsStr) when is_list(hd(TermsStr)) ->
  #dl_atom{pred_sym = cons_const(PredSym), args = cons_args_from_list(TermsStr)};
cons_atom(PredSym, Terms) ->
  #dl_atom{pred_sym = cons_const(PredSym), args = Terms}.

-spec cons_rule(dl_atom(), [dl_atom()]) -> dl_rule().
cons_rule(Head, Body) ->
  #dl_rule{head = Head, body = Body}.

-spec cons_term(string()) -> dl_term().
cons_term(T) when is_list(T) ->
  case is_dl_var(T) of
    true ->
      T;
    false ->
      cons_const(T)
  end.

-spec cons_args_from_list([string() | atom()]) -> [dl_term()].
cons_args_from_list(L) ->
  lists:map(fun cons_term/1, L).

-spec atoms_to_string([dl_atom()]) -> string().
atoms_to_string(Atoms = [#dl_atom{} | _]) ->
  AtomS = lists:map(fun atom_to_string/1, Atoms),
  lists:join("\n", AtomS).

-spec atom_to_string(dl_atom()) -> string().
atom_to_string(A = #dl_atom{pred_sym = Sym}) ->
  io_lib:format("~w(~s)", [Sym, args_to_string(dl_repr:get_atom_args(A))]).

-spec program_to_string(dl_program()) -> string().
program_to_string(P) -> rules_to_string(P).

-spec rules_to_string([dl_rule()]) -> string().
rules_to_string(P = [#dl_rule{} | _]) ->
  Rules = lists:map(fun rule_to_string/1, P),
  lists:join("\n", Rules).


-spec rule_to_string(dl_rule()) -> string().
rule_to_string(#dl_rule{head = Head, body = Body}) ->
  Atoms = lists:map(fun atom_to_string/1, Body),
  atom_to_string(Head) ++ " :- " ++ lists:join(", ", Atoms) ++ ".".


-spec args_to_string([string()]) -> string().
args_to_string(Args) ->
  lists:join(", ", lists:map(fun(A) -> "\"" ++ A ++ "\"" end, Args)).


var_to_string(Var) ->
  Var.

const_to_string(C) when is_atom(C) ->
  atom_to_list(C).

%% from the outside world, should only see strings
-spec get_atom_args(dl_atom()) -> [string()].
get_atom_args(#dl_atom{args = Args}) ->
  lists:map(fun(Arg) ->
               case is_dl_var(Arg) of
                 true -> var_to_string(Arg);
                 false -> const_to_string(Arg)
               end
            end,
            Args).

-spec get_atom_args_by_index([integer()], dl_atom()) -> [dl_const()].
get_atom_args_by_index(Cols, #dl_atom{args = Args}) ->
  Set = sets:from_list(Cols),
  listsi:filteri(fun(_A, Idx) -> sets:is_element(Idx, Set) end, Args).

-spec get_atom_name(dl_atom()) -> string().
get_atom_name(#dl_atom{pred_sym = S}) ->
  atom_to_list(S).

get_rule_head(#dl_rule{head = Head}) ->
  Head.

-spec get_rule_headname(dl_rule()) -> string().
get_rule_headname(#dl_rule{head = Head}) ->
  get_atom_name(Head).

get_rule_body(#dl_rule{body = Body}) ->
  Body.

-spec is_dl_var(dl_term()) -> boolean().
is_dl_var(T) when is_list(T)->
  case string:trim(T, both, "\"") of
    [H|_Tl] when (H >= $a) and (H =< $z) ->
      false;
    [H|_Tl] when (H >= $A) and (H =< $Z) ->
      true
  end;
is_dl_var(_T) -> false.

-spec is_dl_const(dl_term()) -> boolean().
is_dl_const(T) ->
  not is_dl_var(T).
