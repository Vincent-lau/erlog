-module(dl_repr).

-compile(export_all).

-include("../include/data_repr.hrl").

-spec cons_atom(string(), [dl_term()]) -> dl_atom().
cons_atom(PredSym, Terms) ->
  #dl_atom{pred_sym = list_to_atom(PredSym), args = Terms}.

-spec cons_rule(dl_atom(), [dl_atom()]) -> dl_rule().
cons_rule(Head, Body) ->
  #dl_rule{head = Head, body = Body}.

-spec cons_term(string()) -> dl_term().
cons_term(T) when is_list(T) ->
  case is_var(T) of
    true ->
      T;
    false ->
      list_to_atom(T)
  end.

-spec cons_args_from_list([string()]) -> [dl_term()].
cons_args_from_list(L) ->
  lists:map(fun cons_term/1, L).


%% from the outside world, should only see strings
-spec get_atom_args(dl_atom()) -> [string()].
get_atom_args(#dl_atom{args = Args}) -> Args.

get_atom_name(#dl_atom{pred_sym = S}) -> atom_to_list(S).

get_rule_head(#dl_rule{head = Head}) -> Head.

-spec get_rule_headname(dl_rule()) -> string().
get_rule_headname(#dl_rule{head = Head}) -> get_atom_name(Head).

get_rule_body(#dl_rule{body = Body}) -> Body.

is_var([H | _]) ->
  case H of
    _ when (H >= 97) and (H =< 122) ->
      false;
    _ when (H >= 65) and (H =< 90) ->
      true
  end.
