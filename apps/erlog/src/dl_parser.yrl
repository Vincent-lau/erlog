Header "%% Copyright (C)"
"%% @private"
"%% @author Vincent".


Nonterminals dl_prog dl_atoms dl_atom dl_rule dl_fact dl_rule_head dl_rule_body 
  dl_term dl_terms.
Terminals dl_const dl_var '(' ')' ':-' ',' '.'.
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
  cons_dl_rule('$1', '$3').

dl_rule_head -> dl_atom : 
  '$1'.
dl_rule_body -> dl_atoms :
  '$1'.
dl_atoms -> dl_atom ','  dl_atoms : 
  [ '$1' | '$3' ].
dl_atoms -> dl_atom dl_atoms:
  [ '$1' | '$2' ].
dl_atoms -> dl_atom : 
  ['$1'].
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

% XXX use dl_repr to do this
-include("data_repr.hrl").

cons_dl_rule(Head, Body) ->
  % Body2 = lists:map(fun ({_, _, B}) -> B end, Body),
  #dl_rule{head = Head, body = Body}.


cons_dl_atom({_, _, Sym}, Args) ->
  Args2 = lists:map(fun ({_, _, Arg}) -> Arg end, Args),
  #dl_atom{pred_sym = Sym, args = Args2}.

