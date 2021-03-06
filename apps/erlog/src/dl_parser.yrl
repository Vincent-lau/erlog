Header "%% Copyright (C)"
"%% @private"
"%% @author Vincent".


Nonterminals dl_prog dl_atom dl_rule dl_fact dl_rule_head dl_rule_body 
  dl_pred dl_preds dl_term dl_terms.
Terminals dl_const dl_var '(' ')' ':-' ',' '.' '!'.
Rootsymbol dl_prog.
Endsymbol '$end'.

dl_prog -> dl_rule dl_prog : 
  ['$1' | '$2'].

dl_prog -> dl_fact dl_prog :
  ['$1' | '$2'].

dl_prog -> dl_rule :
  ['$1'].
dl_prog -> dl_fact:
  ['$1'].


% dl_prog -> dl_rules :
%   '$1'.
% dl_rules -> dl_rule :
%   ['$1'].
% dl_rules -> dl_rule dl_rules :
%   ['$1' | '$2'].


dl_fact -> dl_rule_head '.' :
  '$1'.

dl_rule -> dl_rule_head ':-' dl_rule_body '.' : 
  dl_repr:cons_rule('$1', '$3').

dl_rule_head -> dl_atom : 
  '$1'.
dl_rule_body -> dl_preds :
  '$1'.
dl_preds -> dl_pred dl_preds :
  [ '$1' | '$2' ].
dl_preds -> dl_pred ',' dl_preds:
  [ '$1' | '$3' ].
dl_preds -> dl_pred :
  [ '$1' ].
dl_pred -> dl_atom:
  dl_repr:cons_pred(false, '$1').
dl_pred -> '!' dl_atom:
  dl_repr:cons_pred(true, '$2').
dl_atom -> dl_const '(' dl_terms ')' : 
  cons_dl_atom('$1', '$3').
dl_terms -> dl_term : 
  [ '$1' ].
dl_terms -> dl_term ',' dl_terms :
  [ '$1' | '$3' ].
dl_term -> dl_const :
  '$1'.
dl_term -> dl_var :
  '$1'.

Erlang code.

cons_dl_atom({_, _, Sym}, Args) ->
  Args2 = lists:map(fun ({_, _, Arg}) -> Arg end, Args),
  dl_repr:cons_atom(Sym, Args2).

