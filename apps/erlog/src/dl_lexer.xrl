Definitions.

Const = [a-z][0-9a-zA-Z_]*
Var = [A-Z_][0-9a-zA-Z_]*
WHITESPACE = [\s\t]
COMMA = (,)
EOL = \n|\r\n|\r
BRAC = \(|\)
TERMINATOR = \.
CONNECT = :-


% D = [0-9]


Rules.


% {D}+ :
%   {token,{integer,TokenLine,list_to_integer(TokenChars)}}.

% {D}+\.{D}+((E|e)(\+|\-)?{D}+)? :
%   {token,{float,TokenLine,list_to_float(TokenChars)}}. 

{WHITESPACE} : skip_token.
{EOL} : skip_token. 
{Const} : 
  {token, {dl_const, TokenLine, list_to_atom(TokenChars)}}.
{Var} :
  {token, {dl_var, TokenLine, TokenChars}}.
{COMMA} : {token, {list_to_atom(TokenChars), TokenLine}}.
{BRAC} : {token, {list_to_atom(TokenChars), TokenLine}}.
{CONNECT} : {token, {list_to_atom(TokenChars), TokenLine}}.
{TERMINATOR} : {token, {list_to_atom(TokenChars), TokenLine}}.
{CONNECT} : {token, {list_to_atom(TokenChars), TokenLine}}.

Erlang code.
