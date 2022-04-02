-module(dl_repr).

-compile(export_all).

-compile(nowarn_export_all).

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
cons_const(S) when is_list(S) ->
  S;
cons_const(A) when is_atom(A) ->
  atom_to_list(A).

-spec cons_atom(string(), [dl_term() | string()]) -> dl_atom().
cons_atom(PredSym, TermsStr) when is_list(hd(TermsStr)) ->
  #dl_atom{pred_sym = cons_const(PredSym), args = cons_args_from_list(TermsStr)};
cons_atom(PredSym, TermsAtom) when is_atom(hd(TermsAtom)) ->
  #dl_atom{pred_sym = cons_const(PredSym), args = lists:map(fun atom_to_list/1, TermsAtom)};
cons_atom(PredSym, Terms) ->
  #dl_atom{pred_sym = cons_const(PredSym), args = Terms}.

-spec cons_pred(dl_atom()) -> dl_pred().
cons_pred(Atom = #dl_atom{}) ->
  cons_pred(false, Atom).

-spec cons_pred(boolean(), dl_atom()) -> dl_pred().
cons_pred(Neg, Atom = #dl_atom{}) ->
  #dl_pred{neg = Neg, atom = Atom}.

-spec is_neg_pred(dl_pred()) -> boolean().
is_neg_pred(#dl_pred{neg = Neg}) ->
  Neg.

-spec is_neg_rule(dl_rule()) -> boolean().
is_neg_rule(#dl_rule{body = Body}) ->
  lists:any(fun is_neg_pred/1, Body).

-spec cons_rule(dl_atom(), [dl_pred() | dl_atom()]) -> dl_rule().
cons_rule(Head,
          Body = [#dl_atom{} | _T]) -> % backwards compat
  NewBody = lists:map(fun cons_pred/1, Body),
  cons_rule(Head, NewBody);
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

-spec cons_args_from_list([string()]) -> [dl_term()].
cons_args_from_list(L) ->
  lists:map(fun cons_term/1, L).

-spec atoms_to_string([dl_atom()]) -> string().
atoms_to_string(Atoms = [#dl_atom{} | _]) ->
  AtomS = lists:map(fun atom_to_string/1, Atoms),
  lists:join("\n", AtomS).

-spec atom_to_string(dl_atom()) -> string().
atom_to_string(A = #dl_atom{pred_sym = Sym}) ->
  io_lib:format("~s(~s)", [Sym, args_to_string(dl_repr:get_atom_args(A))]).

-spec program_to_string(dl_program()) -> string().
program_to_string(P) ->
  rules_to_string(P).

-spec rules_to_string([dl_rule()]) -> string().
rules_to_string(P = [#dl_rule{} | _]) ->
  Rules = lists:map(fun rule_to_string/1, P),
  lists:join("\n", Rules).

-spec rule_to_string(dl_rule()) -> string().
rule_to_string(#dl_rule{head = Head, body = Body}) ->
  Atoms =
    lists:map(fun(#dl_pred{neg = Neg, atom = Atom}) ->
                 case Neg of
                   true -> "!";
                   false -> ""
                 end
                 ++ atom_to_string(Atom)
              end,
              Body),
  atom_to_string(Head) ++ " :- " ++ lists:join(", ", Atoms) ++ ".".

-spec args_to_string([string()]) -> string().
args_to_string(Args) ->
  lists:join(", ", lists:map(fun(A) -> "\"" ++ A ++ "\"" end, Args)).

var_to_string(Var) ->
  Var.

const_to_string(C) when is_atom(C) ->
  atom_to_list(C);
const_to_string(C) when is_list(C) ->
  C.

-spec get_atom_arity(dl_atom()) -> pos_integer().
get_atom_arity(Atom = #dl_atom{}) ->
  length(get_atom_args(Atom)).

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
  const_to_string(S).

-spec get_pred_name(dl_pred()) -> string().
get_pred_name(#dl_pred{atom = Atom}) ->
  get_atom_name(Atom).

get_pred_atom(#dl_pred{atom = Atom}) ->
  Atom.

get_rule_head(#dl_rule{head = Head}) ->
  Head.

-spec get_rule_headname(dl_rule()) -> string().
get_rule_headname(#dl_rule{head = Head}) ->
  get_atom_name(Head).

-spec get_rule_body(dl_rule()) -> [dl_pred()].
get_rule_body(#dl_rule{body = Body}) ->
  Body.

-spec get_rule_body_atoms(dl_rule()) -> [dl_atom()].
get_rule_body_atoms(#dl_rule{body = Body}) ->
  lists:map(fun(#dl_pred{atom = Atom}) -> Atom end, Body).

-spec is_dl_var(dl_term()) -> boolean().
is_dl_var(T) when is_list(T) ->
  case string:trim(T, both, "\"") of
    [H | _Tl] when (H >= $a) and (H =< $z) ->
      false;
    [H | _Tl] when (H >= $A) and (H =< $Z) ->
      true
  end;
is_dl_var(_T) ->
  false.

-spec is_dl_const(dl_term()) -> boolean().
is_dl_const(T) ->
  not is_dl_var(T).
